const kvstore = require('ocore/kvstore.js');

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

exports.getFromKV = getFromKV;
exports.storeIntoKV = storeIntoKV;
