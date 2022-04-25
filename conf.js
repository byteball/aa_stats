/*jslint node: true */
"use strict";

//exports.port = 6611;
//exports.myUrl = 'wss://localhost';
exports.bServeAsHub = false;
exports.bLight = false;
exports.bSingleAddress = true;
exports.bStaticChangeAddress = false;

exports.logToSTDOUT = false;
exports.bNoPassphrase = true;
exports.bFaster = true;

exports.storage = 'sqlite';


exports.hub = process.env.testnet ? 'obyte.org/bb-test' : (process.env.devnet ? 'arbregistry.ngrok.io' : 'obyte.org/bb');
exports.deviceName = 'aa-stats-backend';
exports.permanent_pairing_secret = '0000'; // use '*' to allow any or generate random string
exports.control_addresses = ['DEVICE ALLOWED TO CHAT'];
exports.payout_address = 'WHERE THE MONEY CAN BE SENT TO';
exports.KEYS_FILENAME = 'keys.json';

// where logs are written to (absolute path).  Default is log.txt in app data directory
//exports.LOG_FILENAME = '/dev/null';
// set true to append logs to logfile instead of overwriting it. Default is to overwrite
// exports.appendLogfile = true;


console.log('finished headless conf');
