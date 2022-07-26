const Koa = require('koa');
const KoaRouter = require('koa-router');
const cors = require('@koa/cors');
const bodyParser = require('koa-bodyparser');
const mount = require('koa-mount');

const conf = require('ocore/conf.js');
const db = require('ocore/db.js');
const { getAssetID, getAssetName, assetsMetadata } = require('./assets');

function enrichData(rows, asset) {
	for (let r of rows) {
		const a = r.asset !== undefined ? r.asset : asset;
		if (a !== null && typeof a !== 'string')
			throw Error(`bad asset ${a}, ${asset}`);
		if (asset !== false)
			r.decimals = a ? assetsMetadata[a].decimals : 9;
		if (r.asset)
			r.asset = getAssetName(r.asset);
	}
	return rows;
}

const app = new Koa();
app.use(cors());
app.use(bodyParser());
const apiRouter = new KoaRouter();




/* POST /address => stats for one AA address
req body: {
	"address": "IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA",
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets individually
	"timeframe": "hourly" // "hourly" or "daily"
	"from": 448531, // either hour or day (depending on timeframe) number since unix epoch
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"address":"IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA","timeframe":"hourly","from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/address
*/
apiRouter.all('/address', async ctx => {
	let req = ctx.request.body || ctx.query;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	const timeframe = req.timeframe === "daily" ? "daily" : "hourly";
	let sql = `SELECT
		${timeframe == "hourly" ? "hour" : "day"} AS period,
		address,
		asset,
		amount_in,
		amount_out,
		usd_amount_in,
		usd_amount_out,
		triggers_count,
		bounced_count,
		num_users
		FROM aa_stats_${timeframe}
		WHERE address=?
		AND period BETWEEN ? AND ?`;
	if (asset !== false) {
		sql += ` AND asset IS ?`;
	}
	sql += ` ORDER BY period ASC`;
	const rows = await db.query(sql, [req.address, +req.from, +req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /address/tvl => TVL over time for one AA address
req body: {
	"address": "IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA",
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets 
	"from": 448531, // hour number since unix epoch
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"address":"IFFGFP32MYAQZCBXNGCO3ARF3AM6VTNA","from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/address/tvl
*/
apiRouter.all('/address/tvl', async ctx => {
	let req = ctx.request.body || ctx.query;
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
	const rows = await db.query(sql, [req.address, +req.from, +req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /total/tvl => total TVL over time
req body: {
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets 
	"from": 448531, // hour number since unix epoch
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/total/tvl
*/
apiRouter.all('/total/tvl', async ctx => {
	let req = ctx.request.body || ctx.query;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	let sql = `SELECT
		hour AS period,
		${asset !== false ? 'SUM(balance) AS balance,' : ''}
		SUM(usd_balance) AS usd_balance
		FROM aa_balances_hourly
		WHERE "hour" BETWEEN ? AND ?
		${asset !== false ? 'AND asset IS ?' : ''}
		GROUP BY period`;
	const rows = await db.query(sql, [+req.from, +req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /total/activity => total activity in terms of usd_amount_in / usd_amount_out / num of txs / over time
req body: {
	"timeframe": "hourly", // or "daily"
	"asset": null, // optional, null for bytes, if not preset - returns values for all assets 
	"from": 448531, // hour or day number since unix epoch
	"to": 457593 // same
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"from": 448531,"to":457600}' \
  http://localhost:8080/api/v1/total/activity
*/
apiRouter.all('/total/activity', async ctx => {
	let req = ctx.request.body || ctx.query;
	const timeframe = req.timeframe === "daily" ? "daily" : "hourly";
	const period = timeframe === "daily" ? "day" : "hour";
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	let sql = `SELECT
		${timeframe == "hourly" ? "hour" : "day"} AS period,
		${asset !== false ? 'SUM(amount_in) AS amount_in,' : ''}
		${asset !== false ? 'SUM(amount_out) AS amount_out,' : ''}
		SUM(usd_amount_in) AS usd_amount_in,
		SUM(usd_amount_out) AS usd_amount_out,
		SUM(triggers_count) AS triggers_count,
		SUM(bounced_count) AS bounced_count,
		SUM(num_users) AS num_users
		FROM aa_stats_${timeframe}
		WHERE ${period} BETWEEN ? AND ?
		${asset !== false ? 'AND asset IS ?' : ''}
		GROUP BY ${period}`;
	const rows = await db.query(sql, [+req.from, +req.to, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /top/aa/tvl => Top AAs by TVL
req body: {
	"asset": null, // optional, null for bytes, if not preset - return top TVL in USD
	"period": 448531 // hour number since unix epoch, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/aa/tvl
*/
apiRouter.all('/top/aa/tvl', async ctx => {
	let req = ctx.request.body || ctx.query;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	const hour = "period" in req ? req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	let sql = (asset !== false)
		? `SELECT
			hour AS period,
			address,
			balance,
			usd_balance
			FROM aa_balances_hourly
			WHERE period=? AND asset IS ?
			ORDER BY usd_balance DESC`
		: `SELECT
			hour AS period,
			address,
			SUM(usd_balance) AS usd_balance
			FROM aa_balances_hourly
			WHERE period=?
			GROUP BY address
			ORDER BY usd_balance DESC`;
	const rows = await db.query(sql, [hour, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /top/aa/(amount_in|amount_out|triggers_count|num_users) => Top AAs by usd_amount_in / usd_amount_out / num of txs / num of users
req body: {
	"timeframe": "hourly", // or "daily"
	"limit": "50", // top-N, default = 50
	"asset": null, // required, null for bytes, otherwise asset id
	"from": 448531 // hour or day number since unix epoch, if omitted - returns last hour
	"to": 457689
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"asset": null, "timeframe": "hourly"}' \
  http://localhost:8080/api/v1/top/aa/amount_in
*/
apiRouter.all('/top/aa/:type', async ctx => {
	let type = ctx.params['type'];
	if (!["usd_amount_in", "usd_amount_out", "triggers_count", "num_users"].includes(type))
		ctx.throw(404, 'type is incorrect');
	let req = ctx.request.body || ctx.query;
	const timeframe = req.timeframe === "daily" ? "daily" : "hourly";
	const period = timeframe === "daily" ? "day" : "hour";
	const period_length = timeframe === "daily" ? 1000 * 3600 * 24 : 1000 * 3600;
	const from = typeof req.from === 'number' ? req.from : Math.floor(Date.now() / period_length) - 1;
	const to = typeof req.to === 'number' ? req.to : Math.floor(Date.now() / period_length) - 1;
	const limit = req.limit|0 || 50;
	const asset = "asset" in req ? getAssetID(req.asset) : false;
	let sql = `SELECT
		address,
		${asset !== false ? 'SUM(amount_in) AS amount_in,' : ''}
		${asset !== false ? 'SUM(amount_out) AS amount_out,' : ''}
		SUM(usd_amount_in) AS usd_amount_in,
		SUM(usd_amount_out) AS usd_amount_out,
		SUM(triggers_count) AS triggers_count,
		SUM(bounced_count) AS bounced_count,
		SUM(num_users) AS num_users
		FROM aa_stats_${timeframe}
		WHERE ${period} BETWEEN ? AND ?
		${asset !== false ? 'AND asset IS ?' : ''}
		GROUP BY address ORDER BY ${type} DESC LIMIT ${limit}`
	const rows = await db.query(sql, [from, to, ...(asset !== false ? [asset] : [])]);
	ctx.body = enrichData(rows, asset);
});

/* POST /top/asset/tvl => Top assets by TVL
req body: {
	"limit": "50", // top-N, default = 50
	"period": 457673 // hour number since unix epoch, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/asset/tvl
*/
apiRouter.all('/top/asset/tvl', async ctx => {
	let req = ctx.request.body || ctx.query;
	const hour = "period" in req ? +req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	const limit = req.limit|0 || 50;
	let sql = `SELECT
			hour AS period,
			asset,
			SUM(balance) AS total_balance,
			SUM(usd_balance) AS total_usd_balance
		FROM aa_balances_hourly
		WHERE period=?
		GROUP BY asset
		ORDER BY total_usd_balance DESC LIMIT ${limit}`;
	const rows = await db.query(sql, [hour]);
	ctx.body = enrichData(rows, true);
});

/* POST /top/asset/volume => Top assets by volume (amount_in)
req body: {
	"limit": "50", // top-N, default = 50
	"period": 457673 // hour number since unix epoch, if omitted - returns last hour
}
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{}' \
  http://localhost:8080/api/v1/top/asset/amount_in
*/
apiRouter.all('/top/asset/amount_in', async ctx => {
	let type = ctx.params['type'];
	let req = ctx.request.body || ctx.query;
	const hour = "period" in req ? +req.period : Math.floor(Date.now() / 1000 / 60 / 60)-1;
	const limit = req.limit|0 || 50;
	let sql = `SELECT
			hour AS period,
			asset,
			SUM(amount_in) AS total_amount_in,
			SUM(usd_amount_in) AS total_usd_amount_in
		FROM aa_stats_hourly
		WHERE period=?
		GROUP BY asset
		ORDER BY total_usd_amount_in DESC LIMIT ${limit}`;
	const rows = await db.query(sql, [hour]);
	ctx.body = enrichData(rows, true);
});

app.use(mount('/api/v1', apiRouter.routes()));


async function start() {
	app.listen(conf.webserverPort, () => console.log('Web server started on port ' + conf.webserverPort));
}

exports.start = start;
