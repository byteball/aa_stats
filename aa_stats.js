const eventBus = require('ocore/event_bus.js');
const db = require('ocore/db.js');
const rates = require('ocore/network.js').exchangeRates;
const kvstore = require('ocore/kvstore.js');
const headlessWallet = require('headless-obyte');
const wallet = require('ocore/wallet.js');
const CronJob = require('cron').CronJob;

const kv_hourly_key = 'aa_stats_last_response_id_hourly';
const kv_key = 'aa_stats_last_response_id_';

const Koa = require('koa');
const KoaRouter = require('koa-router');
const cors = require('@koa/cors');
const bodyParser = require('koa-bodyparser');
const mount = require('koa-mount');

const app = new Koa();
app.use(cors());
app.use(bodyParser());
const apiRouter = new KoaRouter();


process.on('unhandledRejection', up => {
	console.error('unhandledRejection event', up);
	setTimeout(() => {throw up}, 100);
//	throw up;
});

let assetsMetadata = {};

async function start() {
	console.log('Starting aa_stats daemon');
	await createTableIfNotExists();
	await aggregatePeriod(60);
	setInterval(() => {aggregatePeriod(60)}, 1000 * 60);
	await aggregatePeriod(60 * 24);
	setInterval(() => {aggregatePeriod(60 * 24)}, 1000 * 60 * 10 + 30 * 1000); // offset by 30 seconds to not interfier with hourly queries
	await snapshotBalances();
	(new CronJob('1 * * * * *', snapshotBalances)).start();
	app.listen(8080, () => {console.log('Web server started on port 8080')});
}

// timeframe is in minutes
async function aggregatePeriod(timeframe) {
	let lastResponseId = await getFromKV(`${kv_key}${timeframe}`) || 0;
	console.log(`aggregatePeriod ${timeframe} last reponse id ${lastResponseId}`);

	// we use two distinct queries as they have different join conditions
	
	const inputRows = await db.query(`SELECT
			MAX(aa_response_id) AS last_response_id,
			units.timestamp / 60 / ? AS period,
			aa_address,
			total_inputs.asset,
			SUM(total_inputs.amount) AS amount_in,
			0 AS amount_out,
			COUNT(1) AS triggers_count,
			SUM(bounced) AS bounced_count,
			COUNT(DISTINCT trigger_address) AS num_users
		FROM aa_responses
		JOIN units ON aa_responses.trigger_unit=units.unit
		JOIN outputs AS total_inputs ON total_inputs.unit=aa_responses.trigger_unit AND total_inputs.address=aa_address
		WHERE aa_response_id > ?
		GROUP BY units.timestamp / 60 / ?, aa_address, total_inputs.asset
		ORDER BY aa_response_id ASC`, [timeframe, lastResponseId, timeframe]);

	const outputRows = await db.query(`SELECT
			MAX(aa_response_id) AS last_response_id,
			units.timestamp / 60 / ? AS period,
			aa_address,
			total_outputs.asset,
			0 AS amount_in,
			SUM(total_outputs.amount) AS amount_out,
			COUNT(1) AS triggers_count,
			SUM(bounced) AS bounced_count,
			COUNT(DISTINCT trigger_address) AS num_users
		FROM aa_responses
		JOIN units ON aa_responses.trigger_unit=units.unit
		JOIN outputs AS total_outputs ON total_outputs.unit=aa_responses.response_unit
		WHERE aa_response_id > ?
		GROUP BY units.timestamp / 60 / ?, aa_address, total_outputs.asset
		ORDER BY aa_response_id ASC`, [timeframe, lastResponseId, timeframe]);

	// now merge two results

	let rows = {};
	const _key = row => '' + row.period + row.aa_address + row.asset;
	for (let row of inputRows) {
		rows[_key(row)] = row;
	}
	for (let row of outputRows) {
		cRow = rows[_key(row)];
		if (cRow == null) {
			rows[_key(row)] = row;
			continue;
		}
		cRow.last_response_id = Math.max(cRow.last_response_id, row.last_response_id);
		cRow.amount_out = row.amount_out;	
	}
	rows = Object.values(rows).sort((a,b) => (a.last_response_id > b.last_response_id) ? 1 : ((b.last_response_id > a.last_response_id) ? -1 : 0));
	
	// request missing asset infos

	let assets = [...new Set(rows.map(r => r.asset))].filter(a => !assetsMetadata.hasOwnProperty(a) && a != null);
	const aMs = await new Promise((resolve, reject) => {
		let aMs;
		wallet.readAssetMetadata(assets, (ams) => {
			aMs = ams;
		}, () => {
			resolve(aMs);
		});
	});
	Object.assign(assetsMetadata, aMs);

	// aggregate

	let lastPeriod = 0;
	let periodRows = [];

	for (const row of rows) {
		if (lastPeriod > 0 && row.period > lastPeriod) { // close this timeframe
			const conn = await db.takeConnectionFromPool();
			await conn.query(`BEGIN`);
			for (const row of periodRows) {
				const usd_amount_in = getUSDAmount(row.asset, row.amount_in);
				const usd_amount_out = getUSDAmount(row.asset, row.amount_out);
				const tableName = `aa_stats_${timeframe === 60 ? 'hourly' : 'daily'}`;
				const periodColumnName = timeframe === 60 ? 'hour' : 'day';
				await conn.query(`
					INSERT INTO ${tableName} (
						${periodColumnName},
						aa_address,
						asset,
						amount_in,
						amount_out,
						usd_amount_in,
						usd_amount_out,
						triggers_count,
						bounced_count,
						num_users
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
						lastPeriod,
						row.aa_address,
						row.asset,
						row.amount_in,
						row.amount_out,
						usd_amount_in,
						usd_amount_out,
						row.triggers_count,
						row.bounced_count,
						row.num_users
					]);
			}
			await conn.query(`COMMIT`);
			conn.release();
			periodRows = [];
			await storeIntoKV(`${kv_key}${timeframe}`, lastResponseId);
			console.log(`Aggregated ${timeframe} minutes: ${lastPeriod}, lastResponseId: ${lastResponseId}`);
		}
		lastPeriod = row.period;
		lastResponseId = Math.max(lastResponseId, row.last_response_id);
		periodRows.push(row);
	}
}

async function snapshotBalances() {
	const lastHour = (await db.query(`SELECT MAX(hour) AS hour FROM aa_balances_hourly`))[0].hour || 0;
	const currentHour = Math.floor(Date.now() / 1000 / 60 / 60);
	if (currentHour <= lastHour)
		return;
	console.log('starting snapshotting of balances', currentHour, lastHour);
	const conn = await db.takeConnectionFromPool();
	await conn.query(`BEGIN`);
	const rows = await conn.query(`SELECT address, asset, balance FROM aa_balances`);
	for (const row of rows) {
		row.asset = row.asset === "base" ? null : row.asset;
		row.usd_balance = getUSDAmount(row.asset, row.balance);
		await conn.query(`INSERT INTO aa_balances_hourly (hour, address, asset, balance, usd_balance) VALUES (?, ?, ?, ?, ?)`,
			[currentHour, row.address, row.asset, row.balance, row.usd_balance]);
	}
	await conn.query(`COMMIT`);
	conn.release();
	console.log(`Snapshot of aa_balances done, hour: ${currentHour}`);
}

function getUSDAmount(asset, amount) {
	let rate;
	if (asset) {
		if (rates[`${asset}_USD`]) {
			rate = rates[`${asset}_USD`] * 100;
			if (assetsMetadata[asset] && assetsMetadata[asset].decimals > 0)
				rate /= Math.pow(10, assetsMetadata[asset].decimals);
		}
	} else if (rates['GBYTE_USD']) { // for bytes it should be cents per byte
		rate = rates['GBYTE_USD'] * 100 / 1e9;
	}
	if (rate)
		return Math.round(amount * rate);
	return null;
}

function getAssetID(asset) {
	for (let id in assetsMetadata) {
		if (id === asset || assetsMetadata[id].name === asset) return id;
	}
	return asset;
}

function getAssetName(asset) {
	for (let id in assetsMetadata) {
		if (id === asset) return assetsMetadata[id].name || asset;
	}
	return asset;
}

function getFromKV(key) {
	return new Promise((resolve, reject) => {
		kvstore.get(key, resolve);
	});
}
function storeIntoKV(key, val) {
	return new Promise((resolve, reject) => {
		kvstore.put(key, val, (err) => {
			if (err) reject(err);
			else resolve();
		});
	});
}


/* POST /address => stats for one AA address
req body: {
	"address": "IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA",
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets individually
	"timeframe": "hourly" // "hourly" or "daily"
	"from": 448531, // either hour or day (depending on timeframe) in unix timestamp format
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"address":"IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA","timeframe":"hourly","from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/address
*/
apiRouter.post('/address', async ctx => {
	let req = ctx.request.body;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	const timeframe = req.timeframe === "daily" ? "daily" : "hourly";
	let sql = `SELECT
		${timeframe == "hourly" ? "hour" : "day"} AS period,
		aa_address AS address,
		asset,
		amount_in,
		amount_out,
		usd_amount_in,
		usd_amount_out,
		triggers_count,
		bounced_count,
		num_users
		FROM aa_stats_${timeframe}
		WHERE aa_address=?
		AND period BETWEEN ? AND ?`;
	if (asset !== false) {
		sql += ` AND asset IS ?`;
	}
	sql += ` ORDER BY period ASC`;
	const rows = await db.query(sql, [req.address, req.from, req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});
});

/* POST /address/tvl => TVL over time for one AA address
req body: {
	"address": "IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA",
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets 
	"from": 448531, // hour in unix timestamp format
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"address":"IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA","from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/address/tvl
*/
apiRouter.post('/address/tvl', async ctx => {
	let req = ctx.request.body;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	let sql = `SELECT
		hour AS period,
		address,
		asset,
		balance,
		usd_balance
		FROM aa_balances_hourly
		WHERE address=?
		AND "hour" BETWEEN ? AND ?`;
	if (asset !== false) {
		sql += ` AND asset IS ?`;
	}
	sql += ` ORDER BY period ASC`;
	const rows = await db.query(sql, [req.address, req.from, req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});;
});

/* POST /top/aa/tvl => Top AAs by TVL
req body: {
	"asset": null, // optional, null for bytes, if not preset - return top TVL in USD
	"period": 448531 // hour in unix timestamp format, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/aa/tvl
*/
apiRouter.post('/top/aa/tvl', async ctx => {
	let req = ctx.request.body;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	const hour = "period" in req ? req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	let sql = `SELECT
		hour AS period,
		address,
		asset,
		balance,
		usd_balance
		FROM aa_balances_hourly
		WHERE period=?`;
	if (asset !== false) {
		sql += ` AND asset IS ?`;
	}
	sql += ` ORDER BY usd_balance DESC, balance DESC`;
	const rows = await db.query(sql, [hour, ...(asset !== false ? [asset] : [])]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});;
});

/* POST /top/aa/(amount_in|amount_out|triggers_count|num_users) => Top AAs by amount_in / amount_out / num of txs / num of users
req body: {
	"timeframe": "hourly", // or "daily"
	"limit": "50", // top-N, default = 50
	"asset": null, // required, null for bytes, otherwise asset id
	"from": 448531 // hour in unix timestamp format, if omitted - returns last hour
	"to": 457689
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"asset": null, "timeframe": "hourly"}' \
  http://localhost:8080/api/v1/top/aa/amount_in
*/
apiRouter.post('/top/aa/:type', async ctx => {
	let type = ctx.params['type'];
	if (!["amount_in", "amount_out", "triggers_count", "num_users"].includes(type))
		ctx.throw(404, 'type is incorrect');
	let req = ctx.request.body;
	const timeframe = req.timeframe === "daily" ? "daily" : "hourly";
	const limit = req.limit|0 || 50;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	let sql = `SELECT
		${timeframe == "hourly" ? "hour" : "day"} AS period,
		aa_address AS address,
		asset,
		amount_in,
		amount_out,
		usd_amount_in,
		usd_amount_out,
		triggers_count,
		bounced_count,
		num_users
		FROM aa_stats_${timeframe}
		WHERE period BETWEEN ? AND ?`;
	if (asset !== false) {
		sql += ` AND asset IS ?`;
	}
	sql += ` ORDER BY ${type} DESC LIMIT ${limit}`
	const rows = await db.query(sql, [req.from, req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});;
});

/* POST /top/asset/market_cap => Top assets by market cap
req body: {
	"limit": "50", // top-N, default = 50
	"period": 457673 // hour in unix timestamp format, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/asset/market_cap
*/
apiRouter.post('/top/asset/market_cap', async ctx => {
	let req = ctx.request.body;
	const hour = "period" in req ? req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	const limit = req.limit|0 || 50;
	let sql = `SELECT
			hour AS period,
			asset,
			SUM(balance) AS total_balance,
			SUM(usd_balance) AS total_usd_balance
		FROM aa_balances_hourly
		WHERE period=?
		GROUP BY asset
		ORDER BY total_balance DESC LIMIT ${limit}`;
	const rows = await db.query(sql, [hour]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});;
});

/* POST /top/asset/volume => Top assets by volume (amount_in)
req body: {
	"limit": "50", // top-N, default = 50
	"period": 457673 // hour in unix timestamp format, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/asset/amount_in
*/
apiRouter.post('/top/asset/amount_in', async ctx => {
	let type = ctx.params['type'];
	let req = ctx.request.body;
	const hour = "period" in req ? req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	const limit = req.limit|0 || 50;
	let sql = `SELECT
			hour AS period,
			asset,
			SUM(amount_in) AS total_amount_in,
			SUM(usd_amount_in) AS total_usd_amount_in
		FROM aa_stats_hourly
		WHERE period=?
		GROUP BY asset
		ORDER BY total_amount_in DESC LIMIT ${limit}`;
	const rows = await db.query(sql, [hour]);
	ctx.body = rows.map(r => {r.asset = getAssetName(r.asset); return r;});;
});

app.use(mount('/api/v1', apiRouter.routes()));

async function createTableIfNotExists() {
	await db.query(`DROP TABLE IF EXISTS aa_stats_hourly`);
	await db.query(`DROP TABLE IF EXISTS aa_stats_daily`);
	await db.query(`DROP TABLE IF EXISTS aa_balances_hourly`);
	await storeIntoKV(`${kv_key}60`, 0);
	await storeIntoKV(`${kv_key}${60*24}`, 0);/**/

	await db.query(`CREATE INDEX IF NOT EXISTS byResponseUnit ON aa_responses(response_unit)`);

	const columns = `
		aa_address CHAR(32) NOT NULL,
		asset CHAR(44) NULL,
		amount_in INT NOT NULL DEFAULT 0,
		amount_out INT NOT NULL DEFAULT 0,
		usd_amount_in INT NULL,
		usd_amount_out INT NULL,
		triggers_count INT NOT NULL DEFAULT 0,
		bounced_count INT NOT NULL DEFAULT 0,
		num_users INT NOT NULL DEFAULT 0
	`;
	await db.query(`
		CREATE TABLE IF NOT EXISTS aa_stats_hourly (
			hour INT NOT NULL,
			${columns},
			UNIQUE (hour, aa_address, asset)
	)`);
	await db.query(`CREATE INDEX IF NOT EXISTS aaStatsByHour ON aa_stats_hourly(hour)`);

	await db.query(`
		CREATE TABLE IF NOT EXISTS aa_stats_daily (
			day INT NOT NULL,
			${columns},
			UNIQUE (day, aa_address, asset)
	)`);
	await db.query(`CREATE INDEX IF NOT EXISTS aaStatsByDay ON aa_stats_daily(day)`);

	await db.query(`
		CREATE TABLE IF NOT EXISTS aa_balances_hourly (
			hour INT NOT NULL,
			address CHAR(32) NOT NULL,
			asset CHAR(44) NULL,
			balance INT NOT NULL DEFAULT 0,
			usd_balance INT NULL,
			UNIQUE (hour, address, asset)
	)`);
	await db.query(`CREATE INDEX IF NOT EXISTS aaBalancesByHour ON aa_balances_hourly(hour)`);
}

const readyPromise = new Promise((resolve, reject) => {
	eventBus.once('headless_wallet_ready', resolve);
});
const ratesPromise = new Promise((resolve, reject) => {
	eventBus.once('rates_updated', resolve);
});
Promise.all([readyPromise, ratesPromise]).then(start);