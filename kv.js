const kvstore = require('ocore/kvstore.js');

const kv_key = 'aa_stats_last_response_id_';

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

exports.kv_key = kv_key;
exports.getFromKV = getFromKV;
exports.storeIntoKV = storeIntoKV;
