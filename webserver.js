const Koa = require('koa');
const KoaRouter = require('koa-router');
const cors = require('@koa/cors');
const bodyParser = require('koa-bodyparser');
const mount = require('koa-mount');

const db = require('ocore/db.js');
const { getAssetID, getAssetName } = require('./assets');


const app = new Koa();
app.use(cors());
app.use(bodyParser());
const apiRouter = new KoaRouter();




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


async function start() {
	app.listen(8080, () => {console.log('Web server started on port 8080')});
}

exports.start = start;
