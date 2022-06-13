var socketio = require('socket.io');
var database = require('./database');
var lib = require('./lib');
var config = require('./config');
// var https = require('https');

module.exports = function (server, game, chat) {
    var io = socketio(server);

    (function () {
        function on (event) {
            game.on(event, function (data) {
                io.to('joined').emit(event, data);
            });
        }

        on('game_starting');
        on('game_started');
        on('game_tick');
        on('game_crash');
        on('got_login_bonus'); // when user got logion bonus
        on('cashed_out');
        on('player_bet');
        on('add_satoshis'); // add balance to user
        on('update_bankroll'); // tell bankroll to user
        on('update_bet_info'); // tell web server the bet, extra bet amount
        on('update_range_info'); // tell web server the range information for range bet
        on('got_first_deposit_fee'); // when user got first deposit fee
        on('setMaintenance');
    })();

    // Forward chat messages to clients.
    chat.on('msg', function (msg) { io.to('joined').emit('msg', msg); });
    chat.on('modmsg', function (msg) { io.to('moderators').emit('msg', msg); });

    io.on('connection', onConnection);

    function onConnection (socket) {
        socket.once('join', function (info, ack) {
            if (typeof ack !== 'function') { return sendError(socket, '[join] No ack function'); }

            if (typeof info !== 'object') { return sendError(socket, '[join] Invalid info'); }

            var ott = info.ott;
            if (ott) {
                if (!lib.isUUIDv4(ott)) { return sendError(socket, '[join] ott not valid'); }

                database.validateOneTimeToken(ott, function (err, user) {
                    if (err) {
                        if (err == 'NOT_VALID_TOKEN') { return ack(err); }
                        return internalError(socket, err, 'Unable to validate ott');
                    }
                    cont(user);
                });
            } else {
                cont(null);
            }

            function cont (loggedIn) {
                if (loggedIn) {
                    loggedIn.superadmin = loggedIn.userclass === 'superadmin';
                    loggedIn.admin = loggedIn.userclass === 'admin' || loggedIn.userclass === 'superadmin';
                    loggedIn.staff = loggedIn.userclass === 'staff';
                    loggedIn.moderator = loggedIn.userclass === 'admin' || loggedIn.userclass === 'moderator';
                    // if(!loggedIn.admin) {
                    //     database.getReplyCheck(loggedIn.id, function(err, res){
                    //         if (err) return ack(err);
                    //     });
                    //     var res = game.gameinfo;
                    // }
                }

                var res = game.getInfo();
                res['chat'] = chat.getHistory(loggedIn);
                res['table_history'] = game.gameHistory.getHistory();
                res['username'] = loggedIn ? loggedIn.username : null;

                if (res['username'] != null) {
                    res['demo'] = loggedIn.demo;
                    res['staff'] = loggedIn.staff;
                }
                if (res['username'] != null && loggedIn.admin == true) {
                    res['admin'] = loggedIn.admin;
                }
                if (res['username'] != null && loggedIn.superadmin == true) {
                    res['superadmin'] = loggedIn.superadmin;
                }

                if (res['username'] != null && loggedIn.is_parent == true) {
                    res['is_parent'] = loggedIn.is_parent;
                }

                res['balance_satoshis'] = loggedIn ? loggedIn.balance_satoshis : null;

                if(config.GAME_CLOSE == true) {
                    var beijing_time_array = (new Date()).toLocaleString('en-US', {timeZone: 'Asia/Shanghai'}).split(' ');
                    if (beijing_time_array[2] == 'AM') {
                        var beijing_time_hour = parseInt(beijing_time_array[1].split(':')[0]);
                        if (beijing_time_hour >= 2 && beijing_time_hour < 6) {
                            res['maintenance'] = true;
                        }
                    } else {
                        res['maintenance'] = false;
                    }
                } else {
                    res['maintenance'] = false;
                }

                // if(loggedIn && (!loggedIn.admin)) {
                //     database.getReplyCheck(loggedIn.id, function(err, reply){
                //         if (err) {
                //             return ack(err);
                //         }
                //         else {
                //             res['reply'] = reply;
                //             ack(null, res);
                //             joined(socket, loggedIn);
                //         }
                //     });
                // }
                ack(null, res);
                joined(socket, loggedIn);
            }
        });

        database.getBankroll(function (err, bankroll) {
            if (err) console.log('Error : getBankroll');
            database.getFakePool(function (err, fakepool) {
                if (err) console.log('Error : getFakePool');
                database.getAgentSysFeePro(function (err, agent_sys_fee_pro) {
                    if (err) console.log('Error : getAgentSysFeePro');

                    io.emit('update_bankroll',
                        {
                            bankroll: bankroll,
                            fakepool: fakepool,
                            agent_sys_fee_pro: agent_sys_fee_pro
                        });
                });
            });
        });

        // bet information to web server
        database.getSyncInfo(function (err, sync_info) {
            if (err) console.log(err);
                io.emit('update_bet_info',
                {
                    min_bet_amount: sync_info.min_bet_amount,
                    max_bet_amount: sync_info.max_bet_amount,
                    min_extra_bet_amount: sync_info.min_extra_bet_amount,
                    max_extra_bet_amount: sync_info.max_extra_bet_amount,
                    extrabet_multiplier: sync_info.extrabet_multiplier,
                    min_range_bet_amount: sync_info.min_range_bet_amount,
                    max_range_bet_amount: sync_info.max_range_bet_amount,
                    bet_mode: sync_info.bet_mode,
                    bet_mode_mobile: sync_info.bet_mode_mobile,
                    show_hash: sync_info.show_hash
                });
        });

        database.getRangeInfo(undefined, function (err, range_info) {
            if (err) console.log(err);
            io.emit('update_range_info', range_info);
        });
    }

    var clientCount = 0;

    function joined (socket, loggedIn) {
        ++clientCount;

        var strIP = socket.handshake.address;
        strIP = strIP.replace('::ffff:', '');
        var dateToday = new Date();
        var isoDate = dateToday.toISOString();
        if (loggedIn) {
            strIP += ':' + loggedIn.username;
        }
        console.log('G: ' + isoDate + ' + ' + strIP + '  online:' + clientCount);

        // var optionsget = {
        //     host : 'api.ipdata.co',
        //     port : 443,
        //     path : '/' + strIP,
        //     method : 'GET'
        // };
        //
        // if (loggedIn)
        // {
        //     strIP += ":" + loggedIn.username;
        // }
        //
        // var reqGet = https.request(optionsget, function(res)
        // {
        //     var body = '';
        //     res.on('data', function(chunk) {
        //         body += chunk;
        //     });
        //
        //     res.on('end', function()
        //     {
        //         if (body.includes('is a private IP address'))
        //         {
        //             console.log('G: ' + isoDate + ' + ' + strIP + ' online:' + clientCount + " :p");
        //         }
        //         else
        //         {
        //             var fbResponse = JSON.parse(body);
        //             console.log('G: ' + isoDate + ' + ' + strIP + ' online:' + clientCount + " :" + fbResponse.country_name + '.' + fbResponse.city);
        //         }
        //     });
        //
        //     res.on('error', function()
        //     {
        //         console.log('G: ' + isoDate + ' + ' + strIP + ' online:' + clientCount + " :p");
        //     });
        // });
        // reqGet.end();

        socket.join('joined');
        if (loggedIn && loggedIn.moderator) {
            socket.join('moderators');
        }

        socket.on('disconnect', function () {
            --clientCount;
            // io.emit('homepage_event', clientCount);
            var strIP = socket.handshake.address;
            strIP = strIP.replace('::ffff:', '');

            var dateToday = new Date();
            var isoDate = dateToday.toISOString();

            // var optionsget = {
            //     host : 'api.ipdata.co',
            //     port : 443,
            //     path : '/' + strIP,
            //     method : 'GET'
            // };

            if (loggedIn) {
                strIP += ':' + loggedIn.username;
            }

            console.log('G: ' + isoDate + ' - ' + strIP + '  online:' + clientCount);

            // var reqGet = https.request(optionsget, function(res)
            // {
            //     var body = '';
            //     res.on('data', function(chunk) {
            //         body += chunk;
            //     });
            //
            //     res.on('end', function()
            //     {
            //         if (body.includes('is a private IP address'))
            //         {
            //             console.log('G: ' + isoDate + ' - ' + strIP + ' online:' + clientCount + " :p");
            //         }
            //         else
            //         {
            //             var fbResponse = JSON.parse(body);
            //             console.log('G: ' + isoDate + ' - ' + strIP + ' online:' + clientCount + " :" + fbResponse.country_name + '.' + fbResponse.city);
            //         }
            //     });
            //
            //     res.on('error', function()
            //     {
            //         console.log('G: ' + isoDate + ' - ' + strIP + ' online:' + clientCount + " :p");
            //     });
            // });
            // reqGet.end();

            if (loggedIn) {
                lib.log('info', 'socket.cash_out [begin] - username:' + loggedIn.username + '   gmae_id:' + game.gameId + '    diconnect');
                console.log('info', 'socket.cash_out [begin] - username:' + loggedIn.username + '   gmae_id:' + game.gameId + '    diconnect');
                game.cashOut(loggedIn, function (err) {
                    if (err && typeof err !== 'string') {
                        lib.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   gmae_id:' + game.gameId);
                        console.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   gmae_id:' + game.gameId);
                        console.log('Error: auto cashing out got: ', err);
                    }

                    if (!err) {
                        lib.log('info', 'socket.cash_out [end] - username:' + loggedIn.username + '   gmae_id:' + game.gameId + '    diconnect');
                        console.log('info', 'socket.cash_out [end] - username:' + loggedIn.username + '   gmae_id:' + game.gameId + '    diconnect');
                    }
                });
            }
        });

        if (loggedIn) {
            socket.on('place_bet', function (amount, extraBet, autoCashOut, rangeBetInfo, ack) {
                amount = Math.round(amount);
                extraBet = Math.round(extraBet);

                if (typeof rangeBetInfo === 'function') {
                    ack = rangeBetInfo;
                    rangeBetInfo = {};
                    rangeBetInfo.amount = 0;
                    rangeBetInfo.id = {};
                    console.log('rangeBet is function ', amount, extraBet, rangeBetInfo);
                } else  {
                    console.log('rangeBet is number', amount, extraBet, rangeBetInfo);
                }

                Object.keys(rangeBetInfo).forEach(function (key) {
                    if(rangeBetInfo[key] == undefined || rangeBetInfo[key] == null ||
                        rangeBetInfo[key] <= 0 || (!lib.isInt(rangeBetInfo[key] / 100)) ||
                        (!lib.isInt(amount / 100)) || (!lib.isInt(extraBet / 100))) {
                        console.log('Bio : game socket return:', '[place_bet] Must place a bet in multiples of 100, got: ' + amount);
                        return sendError(socket, '[place_bet] Must place a bet in multiples of 100, got: ' + amount);
                    }
                });

                // if (amount > 1e8) // 1 BTC limit
                // {
                //     console.log("Bio : game socket return:", '[place_bet] Max bet size is 1 BTC got: ' + amount);
                //     return sendError(socket, '[place_bet] Max bet size is 1 BTC got: ' + amount);
                // }

                if (Object.keys(rangeBetInfo).length == 0 && (!autoCashOut)) {
                    console.log('Bio : game socket return:', '[place_bet] Must Send an autocashout with a bet');
                    return sendError(socket, '[place_bet] Must Send an autocashout with a bet');
                } else if (Object.keys(rangeBetInfo).length == 0 && (!lib.isInt(autoCashOut) || autoCashOut < 100)) {
                    console.log('Bio : game socket return:', '[place_bet] Must Send an auto cashout with a bet');
                    return sendError(socket, '[place_bet] auto_cashout problem');
                }

                if (typeof ack !== 'function') {
                    console.log('Bio : gam e socket return:', '[place_bet] No ack');
                    return sendError(socket, '[place_bet] No ack');
                }

                game.placeBet(loggedIn, amount, extraBet, rangeBetInfo, autoCashOut, function (err) {
                    if (err) {
                        console.log('socket : place_bet : err : ', err, '      logged in :', loggedIn.username, 'user Id :', loggedIn.id);

                        if (typeof err === 'string') {
                            ack(err);
                        } else {
                            console.error('[INTERNAL_ERROR] unable to place bet, got: ', err);
                            ack('INTERNAL_ERROR');
                        }
                        return;
                    }

                    ack(null);
                });
            });
        }

        socket.on('finish_round', function (currentTime, currentPoint, gameId) {
            game.finishRound(currentTime, currentPoint, gameId);
        });

        socket.on('set_next_0', function () {
            game.setNext0();
        });

        socket.on('cash_out', function (ack) {
            if (!loggedIn) { return sendError(socket, '[cash_out] not logged in'); }

            if (typeof ack !== 'function') { return sendError(socket, '[cash_out] No ack'); }
            lib.log('info', 'socket.cash_out [begin] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button');
            console.log('info', 'socket.cash_out [begin] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button');
            game.cashOut(loggedIn, function (err) {
                if (err) {
                    if (typeof err === 'string') {
                        lib.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button --- err === string');
                        console.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button --- err === string');
                        return ack(err);
                    } else {
                        lib.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button --- err === unable to cashout');
                        console.log('error', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button --- err === unable to cashout');
                        return console.log('[INTERNAL_ERROR] unable to cash out: ', err);
                    }
                }

                lib.log('info', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button');
                console.log('info', 'socket.cash_out [end] - username:' + loggedIn.username + '   game_id:' + game.gameId + '    push button');

                ack(null);
            });
        });

        socket.on('say', function (message) {
            if (!loggedIn) { return sendError(socket, '[say] not logged in'); }

            if (typeof message !== 'string') { return sendError(socket, '[say] no message'); }

            if (message.length == 0 || message.length > 500) { return sendError(socket, '[say] invalid message side'); }

            var cmdReg = /^\/([a-zA-z]*)\s*(.*)$/;
            var cmdMatch = message.match(cmdReg);

            if (cmdMatch) {
                var cmd = cmdMatch[1];
                var rest = cmdMatch[2];

                switch (cmd) {
                    case 'shutdown':
                        if (loggedIn.admin) {
                            game.shutDown();
                        } else {
                            return sendErrorChat(socket, 'Not an admin.');
                        }
                        break;
                    case 'mute':
                    case 'shadowmute':
                        if (loggedIn.moderator) {
                            var muteReg = /^\s*([a-zA-Z0-9_\-]+)\s*([1-9]\d*[dhms])?\s*$/;
                            var muteMatch = rest.match(muteReg);

                            if (!muteMatch) { return sendErrorChat(socket, 'Usage: /mute <user> [time]'); }

                            var username = muteMatch[1];
                            var timespec = muteMatch[2] ? muteMatch[2] : '30m';
                            var shadow = cmd === 'shadowmute';

                            chat.mute(shadow, loggedIn, username, timespec,
                                function (err) {
                                    if (err) { return sendErrorChat(socket, err); }
                                });
                        } else {
                            return sendErrorChat(socket, 'Not a moderator.');
                        }
                        break;
                    case 'unmute':
                        if (loggedIn.moderator) {
                            var unmuteReg = /^\s*([a-zA-Z0-9_\-]+)\s*$/;
                            var unmuteMatch = rest.match(unmuteReg);

                            if (!unmuteMatch) { return sendErrorChat(socket, 'Usage: /unmute <user>'); }

                            var username = unmuteMatch[1];
                            chat.unmute(
                                loggedIn, username,
                                function (err) {
                                    if (err) return sendErrorChat(socket, err);
                                });
                        }
                        break;
                    default:
                        socket.emit('msg', {
                            time: new Date(),
                            type: 'error',
                            message: 'Unknown command ' + cmd
                        });
                        break;
                }
                return;
            }

            chat.say(socket, loggedIn, message);
        });
    }

    function sendErrorChat (socket, message) {
        console.warn('Warning: sending client: ', message);
        socket.emit('msg', {
            time: new Date(),
            type: 'error',
            message: message
        });
    }

    function sendError (socket, description) {
        console.warn('Warning: sending client: ', description);
        socket.emit('err', description);
    }

    function internalError (socket, err, description) {
        console.error('[INTERNAL_ERROR] got error: ', err, description);
        socket.emit('err', 'INTERNAL_ERROR');
    }
};
  