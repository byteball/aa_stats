const eventBus = require('ocore/event_bus.js');
const db = require('ocore/db.js');
const kvstore = require('ocore/kvstore.js');
const headlessWallet = require('headless-obyte');

const kv_hourly_key = 'aa_stats_last_response_id_hourly';
const kv_daily_key = 'aa_stats_last_response_id_daily';

async function start() {
	await createTableIfNotExists();
	await aggregateHourly();
	setInterval(aggregateHourly, 1000 * 60);
	await aggregateDaily();
	setInterval(aggregateDaily, 1000 * 60 * 10 + 30 * 1000); // offset by 30 seconds to not interfier with hourly queries
}

async function aggregateHourly() {
	let lastResponseId = await getFromKV(kv_hourly_key) || 0;
	const rows = await db.query(`SELECT
			MAX(aa_response_id) AS last_response_id,
			units.timestamp / 60 / 60 AS hour,
			aa_address,
			inputs.asset,
			COUNT(1) AS triggers_count,
			SUM(bounced) AS bounced_count,
			COUNT(DISTINCT trigger_address) AS num_users
		FROM aa_responses
		JOIN units ON aa_responses.trigger_unit=units.unit
		JOIN inputs ON aa_responses.trigger_unit=inputs.unit
		WHERE aa_response_id > ?
		GROUP BY units.timestamp / 60 / 60, aa_address, inputs.asset
		ORDER BY aa_response_id ASC`, [lastResponseId]);
	let lastHour = 0;
	let hourlyRows = [];
	for (const row of rows) {
		if (lastHour > 0 && row.hour > lastHour) { // close this hour
			for (const row of hourlyRows) {
				await db.query(`
					INSERT INTO aa_stats_hourly (
						hour,
						aa_address,
						asset,
						amount_in,
						amount_out,
						triggers_count,
						bounced_count,
						num_users
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
						lastHour,
						row.aa_address,
						row.asset,
						0,
						0,
						row.triggers_count,
						row.bounced_count,
						row.num_users
					]);
			}
			hourlyRows = [];
			await storeIntoKV(kv_hourly_key, lastResponseId);
			console.log(`Aggregated hour ${lastHour}, lastResponseId: ${lastResponseId}`);
		}
		lastHour = row.hour;
		lastResponseId = Math.max(lastResponseId, row.last_response_id);
		hourlyRows.push(row);
	}
}

async function aggregateDaily() {
	let lastResponseId = await getFromKV(kv_daily_key) || 0;
	const rows = await db.query(`SELECT
			MAX(aa_response_id) AS last_response_id,
			units.timestamp / 60 / 60 / 24 AS day,
			aa_address,
			inputs.asset,
			COUNT(1) AS triggers_count,
			SUM(bounced) AS bounced_count,
			COUNT(DISTINCT trigger_address) AS num_users
		FROM aa_responses
		JOIN units ON aa_responses.trigger_unit=units.unit
		JOIN inputs ON aa_responses.trigger_unit=inputs.unit
		WHERE aa_response_id > ?
		GROUP BY units.timestamp / 60 / 60 / 24, aa_address, inputs.asset
		ORDER BY aa_response_id ASC`, [lastResponseId]);
	let lastDay = 0;
	let dailyRows = [];
	for (const row of rows) {
		if (lastDay > 0 && row.day > lastDay) { // close this day
			for (const row of dailyRows) {
				await db.query(`
					INSERT INTO aa_stats_daily (
						day,
						aa_address,
						asset,
						amount_in,
						amount_out,
						triggers_count,
						bounced_count,
						num_users
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [
						lastDay,
						row.aa_address,
						row.asset,
						0,
						0,
						row.triggers_count,
						row.bounced_count,
						row.num_users
					]);
			}
			dailyRows = [];
			await storeIntoKV(kv_daily_key, lastResponseId);
			console.log(`Aggregated day ${lastDay}, lastResponseId: ${lastResponseId}`);
		}
		lastDay = row.day;
		lastResponseId = Math.max(lastResponseId, row.last_response_id);
		dailyRows.push(row);
	}
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

async function createTableIfNotExists() {
	await db.query(`DROP TABLE IF EXISTS aa_stats_hourly`);
	await db.query(`DROP TABLE IF EXISTS aa_stats_daily`);
	await storeIntoKV(kv_hourly_key, 0);
	await storeIntoKV(kv_daily_key, 0);

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
}

eventBus.once('headless_wallet_ready', start);

