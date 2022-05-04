const eventBus = require('ocore/event_bus.js');
const headlessWallet = require('headless-obyte');
const { createTables } = require('./db_init');
const webserver = require('./webserver');
const aggregation = require('./aggregation');



process.on('unhandledRejection', up => {
	console.error('unhandledRejection event', up);
	setTimeout(() => {throw up}, 100);
//	throw up;
});


async function start() {
	console.log('Starting aa_stats daemon');
	await createTables();
	await aggregation.start();
	webserver.start();
}


const readyPromise = new Promise((resolve, reject) => {
	eventBus.once('headless_wallet_ready', resolve);
});
const ratesPromise = new Promise((resolve, reject) => {
	eventBus.once('rates_updated', resolve);
});
Promise.all([readyPromise, ratesPromise]).then(start);
