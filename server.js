var async = require('async');
var assert = require('assert');
var constants = require('constants');
var fs = require('fs');

var config = require('./server/config');
var socket = require('./server/socket');
var database = require('./server/database');
var Game = require('./server/game');
var Chat = require('./server/chat');
var lib = require('./server/lib');
var GameHistory = require('./server/game_history');
var ip = require('ip');
// var checkip = require('check-ip');
var http = require('http');
var https = require('https');

var _ = require('lodash');

/// / server

var serverHttp = http.createServer();
serverHttp.listen(config.PORT_HTTP_G, function () {
    console.log('G: Started on port ', config.PORT_HTTP_G, ' with HTTP');
    lib.log('success', 'G: Started on port ', config.PORT_HTTP_G, ' with HTTP');
});
var server = serverHttp;

// var strIP = ip.address();
// var bIsPublicIP = checkip(strIP).isPublicIp;
var serverHttps;
if (config.PRODUCTION === config.PRODUCTION_LINUX || config.PRODUCTION === config.PRODUCTION_WINDOWS) {
    var options = {
        key: fs.readFileSync(config.HTTPS_KEY),
        cert: fs.readFileSync(config.HTTPS_CERT),
        requestCert: false,
        rejectUnauthorized: false,
        secureProtocol: 'SSLv23_method',
        secureOptions: constants.SSL_OP_NO_SSLv3 | constants.SSL_OP_NO_SSLv2
    };

    if (config.HTTPS_CA) {
        options.ca = fs.readFileSync(config.HTTPS_CA);
    }

    serverHttps = https.createServer(options);
    serverHttps.listen(config.PORT_HTTPS_G, function () {
        console.log('G: Started on port ', config.PORT_HTTPS_G, ' with HTTPS');
        lib.log('success', 'G: Started on port ', config.PORT_HTTPS_G, ' with HTTPS');
    });

    server = serverHttps;
}

async.parallel([
    database.getGameHistory,
    database.getLastGameInfo,
    database.getBankroll,
    database.clearPlaying
], function (err, results) {
    if (err) {
        console.error('G: Error: Get table history:', err);
        throw err;
    }

    var gameHistory = new GameHistory(results[0]);
    var info = results[1];
    var bankroll = results[2];

    console.log('G: Have a bankroll of: ', bankroll / 1e8, ' btc');

    var lastGameId = info.id;
    var lastHash = info.hash;
    assert(typeof lastGameId === 'number');

    var game = new Game(lastGameId, lastHash, bankroll, gameHistory);
    var chat = new Chat();

    socket(server, game, chat);

    function updateTop5Leaders () {
        database.updateTop5Leaders(function (err) {
            console.log('UPDATE TOP 5 PLAYERS ' + (new Date()).toLocaleTimeString());
        });
    }

    updateTop5Leaders();
    setInterval(updateTop5Leaders, 300000);
});
