const db = require('ocore/db.js');

async function createTableIfNotExists() {
	/*await db.query(`DROP TABLE IF EXISTS aa_stats_hourly`);
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

exports.createTableIfNotExists = createTableIfNotExists;
