const db = require('ocore/db.js');
const storage = require('ocore/storage.js');
const rates = require('ocore/network.js').exchangeRates;
const wallet = require('ocore/wallet.js');
const CronJob = require('cron').CronJob;
const { assetsMetadata } = require('./assets');
const { getFromKV, storeIntoKV, kv_key } = require('./kv');


let aggregationInProgress = {};

async function start() {
	console.log('Starting aggregation');
	await updateAssetMetadata();
	await aggregatePeriod(60);
	setInterval(() => {aggregatePeriod(60)}, 1000 * 60);
	await aggregatePeriod(60 * 24);
	setInterval(() => {aggregatePeriod(60 * 24)}, 1000 * 60 * 10 + 30 * 1000); // offset by 30 seconds to not interfere with hourly queries
	await snapshotBalances();
	(new CronJob('1 * * * * *', snapshotBalances)).start();
}

// timeframe is in minutes
async function aggregatePeriod(timeframe) {
	if (aggregationInProgress[timeframe]) return;
	aggregationInProgress[timeframe] = true;
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
		ORDER BY period ASC`, [timeframe, lastResponseId, timeframe]);

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
		JOIN outputs AS total_outputs ON total_outputs.unit=aa_responses.response_unit AND total_outputs.address!=aa_address
		WHERE aa_response_id > ?
		GROUP BY units.timestamp / 60 / ?, aa_address, total_outputs.asset
		ORDER BY period ASC`, [timeframe, lastResponseId, timeframe]);

	// now merge two results

	let rowsMap = {};
	const _key = row => '' + row.period + row.aa_address + row.asset;
	for (let row of inputRows) {
		rowsMap[_key(row)] = row;
	}
	for (let row of outputRows) {
		let iRow = rowsMap[_key(row)];
		if (iRow == null) {
			rowsMap[_key(row)] = row;
			continue;
		}
		iRow.last_response_id = Math.max(iRow.last_response_id, row.last_response_id);
		iRow.amount_out = row.amount_out;	
	}
	let rows = Object.values(rowsMap).sort((a,b) => (a.period > b.period) ? 1 : ((b.period > a.period) ? -1 : 0));
	
	// request missing asset infos

	let assets = [...new Set(rows.map(r => r.asset))].filter(a => !assetsMetadata.hasOwnProperty(a) && a != null);
	await updateAssetMetadata(assets);

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
						period_start_date,
						address,
						asset,
						amount_in,
						amount_out,
						usd_amount_in,
						usd_amount_out,
						triggers_count,
						bounced_count,
						num_users
					) VALUES (?, ${db.getFromUnixTime('?')}, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
						lastPeriod,
						lastPeriod * 3600 * (timeframe === 60 ? 1 : 24),
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

	aggregationInProgress[timeframe] = false;
}

async function snapshotBalances() {
	const lastHour = (await db.query(`SELECT MAX(hour) AS hour FROM aa_balances_hourly`))[0].hour || 0;
	const currentHour = Math.floor(Date.now() / 1000 / 60 / 60);
	if (currentHour <= lastHour)
		return;
	console.log('starting snapshotting of balances', currentHour, lastHour);
	const conn = await db.takeConnectionFromPool();
	await conn.query(`BEGIN`);
	if (Object.keys(assetsMetadata).length === 0)
		throw Error(`no asset metadata yet`);
	const rows = await conn.query(`SELECT address, asset, balance FROM aa_balances`);
	for (const row of rows) {
		row.asset = row.asset === "base" ? null : row.asset;
		if (row.asset) {
			const { cap, definer_address } = await storage.readAssetInfo(conn, row.asset);
			if (!cap && definer_address === row.address) // skip balances in assets this same AA has defined
				continue;
		}
		row.usd_balance = getUSDAmount(row.asset, row.balance);
		await conn.query(`INSERT INTO aa_balances_hourly (hour, date, address, asset, balance, usd_balance) VALUES (?, ${db.getFromUnixTime('?')}, ?, ?, ?, ?)`,
			[currentHour, currentHour * 3600, row.address, row.asset, row.balance, row.usd_balance]);
	}
	await conn.query(`COMMIT`);
	conn.release();
	console.log(`Snapshot of aa_balances done, hour: ${currentHour}`);
}

async function updateAssetMetadata(assets) {
	const aMs = await new Promise((resolve, reject) => {
		let aMs;
		wallet.readAssetMetadata(assets, (ams) => {
			aMs = ams;
		}, () => {
			resolve(aMs);
		});
	});
	Object.assign(assetsMetadata, aMs);
}

function getUSDAmount(asset, amount) {
	let rate;
	if (asset) {
		if (rates[`${asset}_USD`]) {
			rate = rates[`${asset}_USD`];
			if (assetsMetadata[asset] && assetsMetadata[asset].decimals > 0)
				rate /= Math.pow(10, assetsMetadata[asset].decimals);
		}
	} else if (rates['GBYTE_USD']) {
		rate = rates['GBYTE_USD'] / 1e9;
	}
	if (rate)
		return +(amount * rate).toFixed(2);
	return null;
}

exports.start = start;
