var assert = require('assert');
// var uuid = require('uuid')

var async = require('async');
var lib = require('./lib');
var pg = require('pg');
//var mysql = require('');
var config = require('./config');

// Increase the client pool size. At the moment the most concurrent
// queries are performed when auto-bettors join a newly created
// game. (A game is ended in a single transaction). With an average
// of 25-35 players per game, an increase to 20 seems reasonable to
// ensure that most queries are submitted after around 1 round-trip
// waiting time or less.
pg.defaults.poolSize = 100;

// The default timeout is 30s, or the time from 1.00x to 6.04x.
// Considering that most of the action happens during the beginning
// of the game, this causes most clients to disconnect every ~7-9
// games only to be reconnected when lots of bets come in again during
// the next game. Bump the timeout to 2 min (or 1339.43x) to smooth
// this out.
pg.defaults.poolIdleTimeout = 120000;

pg.types.setTypeParser(20, function (val) { // parse int8 as an integer
    return val === null ? null : parseInt(val);
});

pg.types.setTypeParser(1700, function (val) { // parse numeric as a float
    return val === null ? null : parseFloat(val);
});

var databaseUrl;
if (config.PRODUCTION === config.PRODUCTION_LOCAL) databaseUrl = config.DATABASE_URL_LOCAL;
if (config.PRODUCTION === config.PRODUCTION_LINUX) databaseUrl = config.DATABASE_URL_LINUX;
if (config.PRODUCTION === config.PRODUCTION_WINDOWS) databaseUrl = config.DATABASE_URL_WINDOWS;

console.log('game server connected to db : [', databaseUrl, ']');
lib.log('info', 'game server connected to db : [' + databaseUrl + ']');

// callback is called with (err, client, done)
function connect (callback) {
    return pg.connect(databaseUrl, callback);
}

function query (query, params, callback) {
    // third parameter is optional
    if (typeof params === 'function') {
        callback = params;
        params = [];
    }

    doIt();
    function doIt () {
        connect(function (err, client, done) {
            if (err) return callback(err);

            // console.log("db:query: ", query);

            client.query(query, params, function (err, result) {
                done();
                if (err) {
                    if (err.code === '40P01') {
                        lib.log('warning', 'db.query - Retrying deadlocked transaction:' + query + '   params:' + params);
                        console.log('G: Warning: Retrying deadlocked transaction: ', query, params);
                        return doIt();
                    }
                    return callback(err);
                }

                callback(null, result);
            });
        });
    }
}

function getClient (runner, callback) {
    doIt();

    function doIt () {
        connect(function (err, client, done) {
            if (err) return callback(err);

            function rollback (err) {
                client.query('ROLLBACK', done);

                if (err.code === '40P01') {
                    lib.log('warning', 'db.getClient - Warning: Retrying deadlocked transaction..');
                    console.log('G: Warning: Retrying deadlocked transaction..');
                    return doIt();
                }

                callback(err);
            }

            client.query('BEGIN', function (err) {
                if (err) { return rollback(err); }

                runner(client, function (err, data) {
                    if (err) { return rollback(err); }

                    client.query('COMMIT', function (err) {
                        if (err) { return rollback(err); }

                        done();
                        callback(null, data);
                    });
                });
            });
        });
    }
}

exports.query = query;

pg.on('error', function (err) {
    console.error('G: Error: DB: ', err);
});

// runner takes (client, callback)

// callback should be called with (err, data)
// client should not be used to commit, rollback or start a new transaction

// callback takes (err, data)

exports.getLastGameInfo = function (callback) {
    lib.log('success', 'db.get_last_game_info - [begin]');
    query('SELECT MAX(id) id FROM games', function (err, results) {
        if (err) return callback(err);
        assert(results.rows.length === 1);

        var id = results.rows[0].id;

        if (!id || id < 1e6) {
            lib.log('exception', 'db.get_last_game_info - !id || id < le6');

            return callback(null, {
                id: 1e6 - 1,
                hash: 'c1cfa8e28fc38999eaa888487e443bad50a65e0b710f649affa6718cfbfada4d'
            });
        }

        query('SELECT hash FROM game_hashes WHERE game_id = $1', [id], function (err, results) {
            if (err) return callback(err);

            assert(results.rows.length === 1);

            lib.log('success', 'db.get_last_game_info - hash:' + results.rows[0].hash);
            lib.log('success', 'db.get_last_game_info - [end] ');
            callback(null, {
                id: id,
                hash: results.rows[0].hash
            });
        });
    });
};

/**
 * Get a record of users table by usernaem
 * @author Bio
 * @param callback
 */
exports.getUserByName = function (username, callback) {
    lib.log('success', 'db.get_user_by_name - [begin]   username:' + username);
    assert(username);
    query('SELECT * FROM users WHERE lower(username) = lower($1)', [username], function (err, result) {
        if (err) return callback(err);
        if (result.rows.length === 0) { return callback('USER_DOES_NOT_EXIST'); }

        assert(result.rows.length === 1);
        lib.log('success', 'db.get_user_by_name - [end]   username:' + username);
        callback(null, result.rows[0]);
    });
};

exports.validateOneTimeToken = function (token, callback) {
    lib.log('success', 'db.validate_one_time_token - [begin]   token:' + token);
    assert(token);
    query('WITH t as (UPDATE sessions SET expired = now() WHERE id = $1 AND ott = TRUE RETURNING *)' +
            'SELECT *,(SELECT time_zone FROM t) AS time_zone FROM users WHERE id = (SELECT user_id FROM t)',
    [token], function (err, result) {
        if (err) return callback(err);
        if (result.rowCount === 0) {
            lib.log('success', 'db.validate_one_time_token - [end] not_valid_token');
            return callback('NOT_VALID_TOKEN');
        }
        assert(result.rows.length === 1);
        lib.log('success', 'db.validate_one_time_token - [end]   token:' + token);
        callback(null, result.rows[0]);
    });
};

exports.getReplyCheck = function (userid, callback) {
    lib.log('success', 'db.get_reply_check - [begin]   user_id:' + userid);
    var sql = 'SELECT id, user_id, email, message_to_user, read, reply_check ' +
            'FROM supports WHERE user_id=$1 AND reply_check=$2';

    query(sql, [userid, false], function (err, res) {
        if (err) {
            return callback(err);
        } else {
            lib.log('success', 'db.get_reply_check - [end]   user_id:' + userid);
            return callback(null, res.rows);
        }
    });
};

/**
 * Get Range Info from Range Bet Table with id
 * @author Bio
 * @since 2018.6.3
 * @param id
 * @return {id, range_from, range_to, range_multiplier}
 */
exports.getRangeInfo = function (range_id, callback) {
    if(range_id == 0) {
        var result = {};
        result.range_from = -1;
        result.range_to = -1;
        result.range_multiplier = 0;
        result.id = 0;
        return callback(null, result);
    }
    var where_clause = '';
    if (range_id !== undefined && range_id != null)
        where_clause = 'WHERE id = ' + range_id;
    query('SELECT * FROM range_bet ' + where_clause + ' ORDER BY range_from', function (err, result) {
        if (err) {
            return callback(err);
        }

        if (result.rowCount === 0 ) {
            return callback('NO_RANGE');
        }

        if (range_id !== undefined && range_id != null)
            return callback(null, result.rows[0]);
        return callback(null, result.rows);
    });
};

exports.getRanges = function (callback) {
    query('SELECT * FROM range_bet', function(err, result) {
        if (!err)
            result = result.rows;
        return callback(err, result);
    });
};

/**
 * bet signal
 * @author Bio
 * @param callback
 */
exports.placeBet = function (bet, extraBet, rangeBetInfo, autoCashOut, userId, gameId, callback) {
    lib.log('success', 'db.place_bet - [begin]   bet:' + bet + '   extra_bet:' + extraBet + '   range_bet:' + rangeBetInfo.amount + ',' + rangeBetInfo.id + '   auto_cashout:' + autoCashOut + '   user_id:' + userId + '   game_id:' + gameId);
    bet = parseInt(bet);
    if (isNaN(bet)) bet = 0;
    extraBet = parseInt(extraBet);
    if (isNaN(extraBet)) extraBet = 0;
    rangeBetInfo.amount = parseInt(rangeBetInfo.amount);
    if (isNaN(rangeBetInfo.amount)) rangeBetInfo.amount = 0;

    if ((bet == 0 && rangeBetInfo.amount == 0) || (bet > 0 && rangeBetInfo.amount > 0))
        return callback('PLACE_BET_ERROR');

    assert(typeof bet === 'number');
    assert(typeof autoCashOut === 'number');
    assert(typeof userId === 'number');
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    if (rangeBetInfo.amount > 0 && rangeBetInfo.range_to == -1) {
        rangeBetInfo.range_to = rangeBetInfo.amount;
    }

    getClient(function (client, callback) {
        lib.log('success', 'db.place_bet - update users table [begin]   user_id:' + userId + '   game_id:' + gameId);
        var total_bet_amount = bet + extraBet + rangeBetInfo.amount;
        client.query('UPDATE users ' +
                    'SET balance_satoshis = balance_satoshis - $1, ' +
                    'net_profit = net_profit - $1, ' +
                    'games_played = games_played + 1 ' +
                    'WHERE id = $2 AND balance_satoshis >= $1 RETURNING *',
        [total_bet_amount, userId], function (err, result_1) {
            if (err) return callback(err);
            lib.log('success', 'db.place_bet - update users table [end]   user_id:' + userId + '   game_id:' + gameId);
            if (result_1.rowCount == 0)
                return callback('NOT_ENOUGH_MONEY');

            var balance_satoshis = result_1.rows[0].balance_satoshis + total_bet_amount;
            var demo = result_1.rows[0].demo;
            var userclass = result_1.rows[0].userclass;
            var username = result_1.rows[0].username;

            if (demo == false && (userclass == 'admin' || username == 'madabit' || username == 'staff' || username == 'ex_to_mt_' || username == 'fun_to_mt_' || userclass == 'superadmin' || userclass == 'staff')) {
                demo = true;
            }

            lib.log('success', 'db.place_bet - insert plays table [begin]   user_id:' + userId + '    game_id:' + gameId);
            client.query('INSERT INTO plays(user_id, game_id, bet, extra_bet, range_bet_amount, range_bet_from, range_bet_to, range_bet_multiplier, auto_cash_out, cash_out, balance_satoshis, ' +
                        'demo, username, userclass, user_master_ib, user_parent1, user_parent2, user_parent3) ' +
                        'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, $10, $11, $12, $13, $14, $15, $16, $17) ' +
                        'RETURNING id',
            [userId, gameId, bet, extraBet, rangeBetInfo.amount, rangeBetInfo.range_from, rangeBetInfo.range_to, rangeBetInfo.range_multiplier, autoCashOut, balance_satoshis.toFixed(), demo,
                result_1.rows[0].username, result_1.rows[0].userclass, result_1.rows[0].master_ib,
                result_1.rows[0].parent1, result_1.rows[0].parent2, result_1.rows[0].parent3], function (err, result_2) {
                if (err) return callback(err);
                lib.log('success', 'db.place_bet - insert plays table [end]   user_id:' + userId + '    game_id:' + gameId);
                var playId = result_2.rows[0].id;
                assert(typeof playId === 'number');
                lib.log('success', 'db.place_bet - [end]   user_id:' + userId + '   play_id:' + playId);
                return callback(null, playId);
            });
        });
    }, callback);
};

/*
 * notify the user who have received the login bonus saving records into supports table
 *  (created, read, reply_check parameters are important)
 */
exports.notifyLoginBonus = function (user_id, login_bonus, callback) {
    lib.log('success', 'db.notify_login_bonus - insert table [begin]   user_id:' + user_id + '    login_bonus:' + login_bonus);
    var sql = 'INSERT INTO supports (user_id, message_to_user, read, reply_check) VALUES ($1, $2, true, false)';
    lib.log('success', 'db.notify_login_bonus - insert table [end]   user_id:' + user_id + '    login_bonus:' + login_bonus);
    var message_to_user = 'login_bonus:' + login_bonus;
    query(sql, [user_id, message_to_user], function (err) {
        if (err) return callback(err);
        return callback(null);
    });
};

/*
 * notify the user who have received the funding bonus inserting a new record into supports table
 *  (created, read, reply_check parameters are important)
 */
exports.notifyFundingBonus = function (username, clientname, funding_bonus, callback) {
    lib.log('success', 'db.notifyFundingBonus - [begin]   username:' + username + '   funding_bonus:' + funding_bonus);
    var sql = 'SELECT id FROM users WHERE username=$1';
    query(sql, [username], function (err, res) {
        if (err) return callback(err);

        var user_id = res.rows[0].id;
        var sql = 'INSERT INTO supports (user_id, message_to_user, read, reply_check) VALUES ($1, $2, true, false)';

        var message_to_user = 'funding_bonus:' + funding_bonus + ' from:' + clientname;
        query(sql, [user_id, message_to_user], function (error) {
            if (error) return callback(error);

            lib.log('success', 'db.notifyFundingBonus - [end]   username:' + username + '   clienttname:' + clientname + '   funding_bonus:' + funding_bonus);
            return callback(null);
        });
    });
};

/**
 * Function that is called when the game is finshied
 * @modified Bio
 * @param gameId : id of games table
 * @param crashPoint : the point that the game was crashed
 * @param extrabet_multiplier : multiplier of extrabet ( saved in common table)
 * @param callback
 */
exports.endGame = function (gameId, crashPoint, extrabet_multiplier, callback) {
    crashPoint = Math.round(crashPoint);
    lib.log('success', 'db.endgame - [begin]   game_id:' + gameId + '   crash_point:' + crashPoint + '   extrabet_multiplier:' + extrabet_multiplier);
    getClient(function (client, callback) {
        function calulateCashoutForRangeBetPlayers (game_id, crash_point, callback) {
            var sql =   'UPDATE plays SET cash_out = range_bet_amount * (range_bet_multiplier) ' +
                        'WHERE game_id = $1 AND bet = 0 AND extra_bet = 0 AND ' +
                        '$2 >= range_bet_from AND $2 <= range_bet_to';

            query(sql, [game_id, crash_point], function (err) {
                if(err) return callback(err);
                return callback(null);
            });
        }

        calulateCashoutForRangeBetPlayers(gameId, crashPoint, function (err) {
            if (err) {
                return callback(err);
            }

            // if (extraBet > 0 && crashPoint == 0) -> extraBetSuccess
            extrabet_multiplier = parseInt(extrabet_multiplier);

            assert(typeof gameId === 'number');
            assert(typeof callback === 'function');

            lib.log('success', 'db.endgame - update game ended = true - [begin]    game_id:' + gameId);
            client.query('UPDATE games SET ended=true, game_crash = $1 WHERE id = $2', [crashPoint, gameId], // get the ended game infomation by id // game_crash = crashPoint // 201800424
                function (err) {
                    if (err) return callback(new Error('Could not end game, got: ' + err));
                    lib.log('success', 'db.endgame - update game ended = true - [end]    game_id:' + gameId);

                    var totalUserProfitMap = {}; // the map that shows each profit for each users
                    // (player, master_id, agent, parent1-3,  company, staff)
                    //  *** for only view in client-side, not database ***
                    var forbiddenAgentMap = {}; // the map that has the forbidden username of agent.

                    var agentProfitPercent = {}; // the map that has the percent of agent system
                    agentProfitPercent['agent_percent_company'] = 0;
                    agentProfitPercent['agent_percent_staff'] = 0;
                    agentProfitPercent['agent_percent_masterib'] = 0;
                    agentProfitPercent['agent_percent_agent'] = 0;
                    agentProfitPercent['agent_percent_parent1'] = 0;
                    agentProfitPercent['agent_percent_parent2'] = 0;
                    agentProfitPercent['agent_percent_parent3'] = 0;
                    agentProfitPercent['agent_percent_player'] = 0;

                    lib.log('success', 'db.endgame - get agent_profit_percent - [begin]    game_id:' + gameId);
                    var sql = "SELECT * FROM common WHERE strkey LIKE 'agent_%'";
                    query(sql, function (err, agentRows) {
                        lib.log('success', 'db.endgame - get agent_profit_percent - [end]    game_id:' + gameId);
                        var agent_percent_player = 100;
                        for (var i = 0; i < agentRows.rows.length; i++) {
                            agentProfitPercent[agentRows.rows[i]['strkey']] = agentRows.rows[i]['strvalue'];
                            agent_percent_player -= agentRows.rows[i]['strvalue'];
                            agentProfitPercent['agent_percent_player'] = agent_percent_player;
                        }

                        // calculate agent profit for busted players
                        function calculateProfitForBustedPlayers (play_info, callback) {
                            lib.log('success', 'calc_busted_players function - [begin]    user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                            var gameId = play_info.game_id;
                            var userId = play_info.user_id;
                            var extraBet = play_info.extra_bet;

                            var username = play_info.username;
                            var user_master_ib = play_info.user_master_ib;
                            var user_parent1 = play_info.user_parent1;
                            var user_parent2 = play_info.user_parent2;
                            var user_parent3 = play_info.user_parent3;

                            var real_profit_for_player = 0;

                            var dispenseVolume = play_info.bet + play_info.extra_bet;

                            var profit_for_player = 0;
                            if (crashPoint == 0 && extraBet > 0) {
                                profit_for_player = play_info.extra_bet * extrabet_multiplier;
                            } // profit / 100 * agentProfitPercent['agent_percent_player'];

                            var profit_for_staff = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_staff']);
                            var profit_for_master_ib = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_masterib']);
                            var profit_for_agent = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_agent']);
                            var profit_for_parent1 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent1']);
                            var profit_for_parent2 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent2']);
                            var profit_for_parent3 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent3']);
                            var profit_for_company = Math.round(dispenseVolume / 100 * (100 - agentProfitPercent['agent_percent_player'])) - (profit_for_staff + profit_for_master_ib + profit_for_agent + profit_for_parent1 + profit_for_parent2 + profit_for_parent3);

                            if ((play_info.userclass == 'agent' || play_info.userclass == 'master_ib') && forbiddenAgentMap[username] != '')
                                ;
                            else {
                                profit_for_company += profit_for_agent;
                                profit_for_agent = 0;
                            }

                            if (crashPoint == 0 && extraBet > 0) {
                                real_profit_for_player = profit_for_player + profit_for_agent;
                            } else real_profit_for_player = profit_for_agent;

                            if (user_master_ib == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_master_ib] == '') {
                                profit_for_company += profit_for_master_ib;
                                profit_for_master_ib = 0;
                            }

                            if (user_parent1 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent1] == '') {
                                profit_for_company += profit_for_parent1;
                                profit_for_parent1 = 0;
                            }

                            if (user_parent2 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent2] == '') {
                                profit_for_company += profit_for_parent2;
                                profit_for_parent2 = 0;
                            }

                            if (user_parent3 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent3] == '') {
                                profit_for_company += profit_for_parent3;
                                profit_for_parent3 = 0;
                            }

                            if (play_info.demo == true) { // demo's profit can't puls to profit for company and staff
                                profit_for_company = 0;
                                profit_for_staff = 0;
                            }

                            var userProfitMap = {}; // profit in agent system from a player

                            if (crashPoint == 0 && extraBet > 0) {
                                userProfitMap.profit_for_player = profit_for_player + play_info.bet + play_info.extra_bet + profit_for_agent;
                            } else {
                                userProfitMap.profit_for_player = profit_for_player + profit_for_agent;
                            }

                            userProfitMap.profit_for_parent1 = profit_for_parent1;
                            userProfitMap.profit_for_parent2 = profit_for_parent2;
                            userProfitMap.profit_for_parent3 = profit_for_parent3;
                            userProfitMap.profit_for_master_ib = profit_for_master_ib;
                            userProfitMap.profit_for_company = profit_for_company;
                            userProfitMap.profit_for_staff = profit_for_staff;

                            userProfitMap.user_parent1 = user_parent1;
                            userProfitMap.user_parent2 = user_parent2;
                            userProfitMap.user_parent3 = user_parent3;
                            userProfitMap.user_master_ib = user_master_ib;

                            totalUserProfitMap[username] = userProfitMap;

                            lib.log('success', 'calc_busted_players - update plays table - [begin]    user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                            sql = 'UPDATE plays SET game_id = $1, user_id = $2, ' +
                                'profit_for_player = $3, ' +
                                'profit_for_company = $4, ' +
                                'profit_for_staff = $5, ' +
                                'profit_for_master_ib = $6, ' +
                                'profit_for_agent = $7, ' +
                                'profit_for_parent1 = $8, ' +
                                'profit_for_parent2 = $9, ' +
                                'profit_for_parent3 = $10 ' +
                                'WHERE id = $11';

                            /* client. */
                            query(sql, [gameId, userId, real_profit_for_player, // save detail in plays table
                                profit_for_company, profit_for_staff,
                                profit_for_master_ib, profit_for_agent,
                                profit_for_parent1, profit_for_parent2,
                                profit_for_parent3, play_info.id],
                            function (err, result) {
                                if (err) {
                                    return callback(err);
                                }
                                lib.log('success', 'calc_busted_players - update plays table - [end]    user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                var tasks = [];
                                if (userProfitMap.profit_for_player != 0) { // trigger event for webserver game page to show satoshis changes in realtime.
                                    tasks.push(function (callback) {
                                        addSatoshis(client, userId, userProfitMap.profit_for_player, 0, profit_for_agent, play_info, callback);
                                    });
                                }

                                if (play_info.demo == false || play_info.demo == null) {
                                    if (userProfitMap.profit_for_master_ib != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_master_ib, userProfitMap.profit_for_master_ib, 0, callback);
                                        });
                                    }
                                    if (userProfitMap.profit_for_parent1 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent1, userProfitMap.profit_for_parent1, 0, callback);
                                        });
                                    }
                                    if (userProfitMap.profit_for_parent2 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent2, userProfitMap.profit_for_parent2, 0, callback);
                                        });
                                    }
                                    if (userProfitMap.profit_for_parent3 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent3, userProfitMap.profit_for_parent3, 0, callback);
                                        });
                                    }
                                    if (userProfitMap.profit_for_company != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, 'madabit', userProfitMap.profit_for_company, 0, callback);
                                        });
                                    }
                                    if (userProfitMap.profit_for_staff != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, 'staff', userProfitMap.profit_for_staff, 0, callback);
                                        });
                                    }
                                }

                                lib.log('success', 'calc_busted_players async.series - [begin]   user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                async.series(tasks, function (err, result) {
                                    if (err) {
                                        return callback(err);
                                    }
                                    lib.log('success', 'calc_busted_players async.series - [end]   user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                    lib.log('success', 'db.endgame - [end] game_id:' + gameId + '   crash_point:' + crashPoint + '   extrabet_multiplier:' + extrabet_multiplier);
                                    callback(null);
                                });
                            });
                        }

                        // calculate agent profit for cashed-out(bet-succeeded) players
                        function calculateProfitForCashedOutPlayers (play_info, callback) {
                            lib.log('success', 'calc_cashed_players function - [begin]   userId:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                            var userId = play_info.user_id;
                            var username = play_info.username;
                            var user_master_ib = play_info.user_master_ib;
                            var user_parent1 = play_info.user_parent1;
                            var user_parent2 = play_info.user_parent2;
                            var user_parent3 = play_info.user_parent3;
                            // var extraBet = play_info.extra_bet;

                            var dispenseVolume = play_info.bet + play_info.extra_bet;

                            var profit_for_staff = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_staff']);
                            var profit_for_master_ib = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_masterib']);
                            var profit_for_agent = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_agent']);
                            var profit_for_parent1 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent1']);
                            var profit_for_parent2 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent2']);
                            var profit_for_parent3 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent3']);
                            var profit_for_company = Math.round(dispenseVolume / 100 * (100 - agentProfitPercent['agent_percent_player'])) - (profit_for_staff + profit_for_master_ib + profit_for_agent + profit_for_parent1 + profit_for_parent2 + profit_for_parent3);

                            if (user_master_ib == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_master_ib] == '') {
                                profit_for_company += profit_for_master_ib;
                                profit_for_master_ib = 0;
                            }

                            if (user_parent1 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent1] == '') {
                                profit_for_company += profit_for_parent1;
                                profit_for_parent1 = 0;
                            }

                            if (user_parent2 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent2] == '') {
                                profit_for_company += profit_for_parent2;
                                profit_for_parent2 = 0;
                            }

                            if (user_parent3 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent3] == '') {
                                profit_for_company += profit_for_parent3;
                                profit_for_parent3 = 0;
                            }

                            if ((play_info.userclass == 'agent' || play_info.userclass == 'master_ib') && forbiddenAgentMap[username] != '')
                                ;
                            else {
                                profit_for_company += profit_for_agent;
                                profit_for_agent = 0;
                            }

                            if (play_info.demo == true) {
                                profit_for_company = 0;
                                profit_for_staff = 0;
                            }

                            var profit_for_player = play_info.cash_out - play_info.bet - play_info.extra_bet + profit_for_agent;

                            var userProfitMap = {};
                            userProfitMap.profit_for_player = profit_for_player + play_info.bet + play_info.extra_bet;
                            userProfitMap.profit_for_company = profit_for_company;
                            userProfitMap.profit_for_staff = profit_for_staff;
                            userProfitMap.profit_for_master_ib = profit_for_master_ib;
                            userProfitMap.profit_for_parent1 = profit_for_parent1;
                            userProfitMap.profit_for_parent2 = profit_for_parent2;
                            userProfitMap.profit_for_parent3 = profit_for_parent3;

                            userProfitMap.user_master_ib = user_master_ib;
                            userProfitMap.user_parent1 = user_parent1;
                            userProfitMap.user_parent2 = user_parent2;
                            userProfitMap.user_parent3 = user_parent3;

                            totalUserProfitMap[play_info['username']] = userProfitMap;

                            lib.log('success', 'calc_cashed_players - update_plays_table_for_profit [begin]   userId: ' + play_info.user_id + '   usrename:' + play_info.username + '   game_id:' + play_info.game_id);
                            sql = 'UPDATE plays SET game_id = $1, user_id = $2, ' +
                                'profit_for_player = $3, ' +
                                'profit_for_company = $4, ' +
                                'profit_for_staff = $5, ' +
                                'profit_for_master_ib = $6, ' +
                                'profit_for_agent = $7, ' +
                                'profit_for_parent1 = $8, ' +
                                'profit_for_parent2 = $9, ' +
                                'profit_for_parent3 = $10 ' +
                                'WHERE id = $11';

                            /* client. */
                            query(sql, [play_info.game_id, play_info.user_id,
                                    profit_for_player, profit_for_company,
                                    profit_for_staff, profit_for_master_ib,
                                    profit_for_agent, profit_for_parent1,
                                    profit_for_parent2, profit_for_parent3, play_info.id],
                                function (err, result) {
                                    if (err) {
                                        return callback(err);
                                    }

                                    lib.log('success', 'calc_cashed_players update_plays_table_for_profit [end] userId:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);

                                    var tasks = [];

                                    tasks.push(function (callback) {
                                        addSatoshis(client, userId, play_info.cash_out + profit_for_agent, 0, profit_for_agent, play_info, callback);
                                    });

                                    if (play_info.demo == false || play_info.demo == null) {
                                        if (profit_for_company != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, 'madabit', profit_for_company, 0, callback);
                                            });
                                        }
                                        if (profit_for_staff != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, 'staff', profit_for_staff, 0, callback);
                                            });
                                        }
                                        if (profit_for_master_ib != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, user_master_ib, profit_for_master_ib, 0, callback);
                                            });
                                        }
                                        if (profit_for_parent1 != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, user_parent1, profit_for_parent1, 0, callback);
                                            });
                                        }
                                        if (profit_for_parent2 != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, user_parent2, profit_for_parent2, 0, callback);
                                            });
                                        }
                                        if (profit_for_parent3 != 0) {
                                            tasks.push(function (callback) {
                                                addSatoshisWithUsername(client, user_parent3, profit_for_parent3, 0, callback);
                                            });
                                        }
                                    }
                                    lib.log('success', 'calc_cashed_players async.series - [begin]   user_id: ' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                    async.series(tasks, function (err, result) {
                                        if (err) {
                                            return callback(err);
                                        }
                                        lib.log('success', 'calc_cashed_players async.series - [end]   user_id:' + play_info.user_id + ', ' + play_info.username + '   game_id:' + play_info.game_id);
                                        lib.log('success', 'calc_cashed_players function - [end]   user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                        callback(null, totalUserProfitMap);
                                    });
                                });
                        }

                        // calculate agent profit for range-beted(succeded or failed) players
                        function calculateProfitForRangeBetedPlayers (play_info, callback) {
                            lib.log('success', 'calc_range_beted_players function - [begin]   userId:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                            var userId = play_info.user_id;
                            var username = play_info.username;
                            var user_master_ib = play_info.user_master_ib;
                            var user_parent1 = play_info.user_parent1;
                            var user_parent2 = play_info.user_parent2;
                            var user_parent3 = play_info.user_parent3;
                            // var extraBet = play_info.extra_bet;

                            var dispenseVolume = play_info.range_bet_amount;

                            var profit_for_staff = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_staff']);
                            var profit_for_master_ib = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_masterib']);
                            var profit_for_agent = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_agent']);
                            var profit_for_parent1 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent1']);
                            var profit_for_parent2 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent2']);
                            var profit_for_parent3 = Math.round(dispenseVolume / 100 * agentProfitPercent['agent_percent_parent3']);
                            var profit_for_company = Math.round(dispenseVolume / 100 * (100 - agentProfitPercent['agent_percent_player'])) - (profit_for_staff + profit_for_master_ib + profit_for_agent + profit_for_parent1 + profit_for_parent2 + profit_for_parent3);

                            if (user_master_ib == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_master_ib] == '') {
                                profit_for_company += profit_for_master_ib;
                                profit_for_master_ib = 0;
                            }

                            if (user_parent1 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent1] == '') {
                                profit_for_company += profit_for_parent1;
                                profit_for_parent1 = 0;
                            }

                            if (user_parent2 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent2] == '') {
                                profit_for_company += profit_for_parent2;
                                profit_for_parent2 = 0;
                            }

                            if (user_parent3 == null || forbiddenAgentMap[username] == '' || forbiddenAgentMap[user_parent3] == '') {
                                profit_for_company += profit_for_parent3;
                                profit_for_parent3 = 0;
                            }

                            if ((play_info.userclass == 'agent' || play_info.userclass == 'master_ib') && forbiddenAgentMap[username] != '')
                                ;
                            else {
                                profit_for_company += profit_for_agent;
                                profit_for_agent = 0;
                            }

                            if (play_info.demo == true) {
                                profit_for_company = 0;
                                profit_for_staff = 0;
                            }

                            var profit_for_player = (((play_info.cash_out - play_info.range_bet_amount) < 0) ? 0 : (play_info.cash_out - play_info.range_bet_amount)) + profit_for_agent;

                            var userProfitMap = {};
                            userProfitMap.profit_for_player = play_info.cash_out + profit_for_agent;
                            userProfitMap.profit_for_company = profit_for_company;
                            userProfitMap.profit_for_staff = profit_for_staff;
                            userProfitMap.profit_for_master_ib = profit_for_master_ib;
                            userProfitMap.profit_for_parent1 = profit_for_parent1;
                            userProfitMap.profit_for_parent2 = profit_for_parent2;
                            userProfitMap.profit_for_parent3 = profit_for_parent3;

                            userProfitMap.user_master_ib = user_master_ib;
                            userProfitMap.user_parent1 = user_parent1;
                            userProfitMap.user_parent2 = user_parent2;
                            userProfitMap.user_parent3 = user_parent3;

                            totalUserProfitMap[play_info['username']] = userProfitMap;

                            lib.log('success', 'calc_cashed_players - update_plays_table_for_profit [begin]   userId: ' + play_info.user_id + '   usrename:' + play_info.username + '   game_id:' + play_info.game_id);
                            sql = 'UPDATE plays SET game_id = $1, user_id = $2, ' +
                                'profit_for_player = $3, ' +
                                'profit_for_company = $4, ' +
                                'profit_for_staff = $5, ' +
                                'profit_for_master_ib = $6, ' +
                                'profit_for_agent = $7, ' +
                                'profit_for_parent1 = $8, ' +
                                'profit_for_parent2 = $9, ' +
                                'profit_for_parent3 = $10 ' +
                                'WHERE id = $11';

                            /* client. */
                            client.query(sql, [play_info.game_id, play_info.user_id,
                                profit_for_player, profit_for_company,
                                profit_for_staff, profit_for_master_ib,
                                profit_for_agent, profit_for_parent1,
                                profit_for_parent2, profit_for_parent3, play_info.id],
                            function (err, result) {
                                if (err) {
                                    return callback(err);
                                }

                                lib.log('success', 'calc_cashed_players update_plays_table_for_profit [end] userId:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);

                                var tasks = [];

                                tasks.push(function (callback) {
                                    addSatoshis(client, userId, play_info.cash_out + profit_for_agent, 0, profit_for_agent, play_info, callback);
                                });

                                if (play_info.demo == false || play_info.demo == null) {
                                    if (profit_for_company != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, 'madabit', profit_for_company, 0, callback);
                                        });
                                    }
                                    if (profit_for_staff != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, 'staff', profit_for_staff, 0, callback);
                                        });
                                    }
                                    if (profit_for_master_ib != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_master_ib, profit_for_master_ib, 0, callback);
                                        });
                                    }
                                    if (profit_for_parent1 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent1, profit_for_parent1, 0, callback);
                                        });
                                    }
                                    if (profit_for_parent2 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent2, profit_for_parent2, 0, callback);
                                        });
                                    }
                                    if (profit_for_parent3 != 0) {
                                        tasks.push(function (callback) {
                                            addSatoshisWithUsername(client, user_parent3, profit_for_parent3, 0, callback);
                                        });
                                    }
                                }
                                lib.log('success', 'calc_cashed_players async.series - [begin]   user_id: ' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                async.series(tasks, function (err, result) {
                                    if (err) {
                                        return callback(err);
                                    }
                                    lib.log('success', 'calc_cashed_players async.series - [end]   user_id:' + play_info.user_id + ', ' + play_info.username + '   game_id:' + play_info.game_id);
                                    lib.log('success', 'calc_cashed_players function - [end]   user_id:' + play_info.user_id + '   username:' + play_info.username + '   game_id:' + play_info.game_id);
                                    callback(null, totalUserProfitMap);
                                });
                            });
                        }

                        sql = 'SELECT * FROM plays p WHERE game_id = $1';

                        lib.log('success', 'db.endgame - get_plays_info [begin]   game_id:' + gameId);
                        query(sql, [gameId], function (err, plays) {
                            lib.log('success', 'db.endGame - get_plays_info [end]   game_id:' + gameId);
                            var no_commission_from = 1, no_commission_to = 1.5;
                            sql = "SELECT * FROM common WHERE strkey LIKE 'no_commission_%'";
                            query(sql, function (err, no_commissions) { // get forbidden commission range from common table.
                                no_commissions = no_commissions.rows;

                                for (var i = 0; i < no_commissions.length; i++) {
                                    if (no_commissions[i]['strkey'] == 'no_commission_from') {
                                        no_commission_from = parseFloat(no_commissions[i]['strvalue']);
                                    }
                                    if (no_commissions[i]['strkey'] == 'no_commission_to') {
                                        no_commission_to = parseFloat(no_commissions[i]['strvalue']);
                                    }
                                }

                                /**
                                 *  Forbidden Agent Player.
                                 *  When Agents or MasterIBs play a game, if their cashed out point is in No_Commission_Range
                                 *  they can't get the agent profit from their children.
                                 *  Also children are forbidden, they can't give agent profit to their parents.
                                 */
                                var tasks = [];
                                var playList = plays.rows;

                                for (i = 0; i < playList.length; i++) {
                                    var play_info = playList[i];
                                    if ( // if the agent or master_ib is fobidden Agent
                                    // (play_info.userclass != 'agent' && play_info.userclass != 'master_ib') ||
                                    (play_info.cash_out == 0 && crashPoint == 0) ||
                                    (play_info.auto_cash_out >= no_commission_from && play_info.auto_cash_out <= no_commission_to) ||
                                    ((play_info.cash_out / play_info.bet) >= no_commission_from && (play_info.cash_out / play_info.bet) < no_commission_to)
                                    ) {
                                        console.log('= set as forbidden', play_info.username);
                                        lib.log('success', 'db.endgame - set_as_forbidden   username:' + play_info.username + '   game_id:' + gameId);
                                        forbiddenAgentMap[play_info.username] = '';
                                    }
                                }

                                playList.forEach(function (play_info, index) {
                                    var cash_out = play_info['cash_out'];

                                    if(play_info.range_bet_amount != 0) {
                                        tasks.push(function (callback) {
                                            calculateProfitForRangeBetedPlayers(play_info, callback);
                                        });
                                    }
                                    else if (cash_out == 0) {
                                        tasks.push(function (callback) {
                                            calculateProfitForBustedPlayers(play_info, callback);
                                        });
                                    } else {
                                        tasks.push(function (callback) {
                                            calculateProfitForCashedOutPlayers(play_info, callback);
                                        });
                                    }
                                });

                                lib.log('success', 'db.endgame - run async.series [begin]   game_id:' + gameId);
                                async.series(tasks, function (err, result) {
                                    if (err) {
                                        return callback(err);
                                    }

                                    if (tasks.length != 0) {
                                        console.log('copy prev balance start', gameId);
                                        sql = 'UPDATE users SET prev_balance_satoshis = balance_satoshis';
                                        client.query(sql, [], function (err) {
                                            console.log('copy prev balance end', gameId);
                                            if (err) callback(err);
                                            lib.log('success', 'G: db.endGame - Run async.series [end] - game_id' + gameId);
                                            lib.log('success', 'G: db.endGame - [end]   game_id:' + gameId + '   crashPoint:' + crashPoint + '  extrabet_multiplier:' + extrabet_multiplier + '   ' + crashPoint + ', ' + extrabet_multiplier);
                                            callback(null, totalUserProfitMap);
                                        });
                                    } else {
                                        lib.log('success', 'G: db.endGame - Run async.series [end] - game_id                                                                                                                                                                                                                                                                                                            ' + gameId);
                                        lib.log('success', 'G: db.endGame - [end]   game_id:' + gameId + '   crashPoint:' + crashPoint + '  extrabet_multiplier:' + extrabet_multiplier + '   ' + crashPoint + ', ' + extrabet_multiplier);
                                        callback(null, totalUserProfitMap);
                                    }
                                });
                            });
                        });
                    });
                });
        });
    }, callback);
};

/**
 * Plus Balance-Satoshis to user with user_id in users table
 * @modified Bio
 */
function addSatoshis (client, userId, amount, extraBet, agent_profit, play_info, callback) {
    lib.log('success', 'add_satoshis - [begin]   user_id:' + userId + '   amount:' + amount + '   extra_bet:' + extraBet + '   agent_profit:' + agent_profit);
    if (extraBet == undefined || extraBet == null) extraBet = 0;

    var total_amount = amount + extraBet;
    var net_profit = play_info.cash_out - play_info.bet - play_info.extra_bet - play_info.range_bet_amount;
    var gross_profit = net_profit > 0 ? net_profit : 0;

    client.query('UPDATE users ' +
                'SET balance_satoshis = balance_satoshis + $1, ' +
                'gross_profit = gross_profit + $2, ' +
                'net_profit = net_profit + $3, ' +
                'agent_profit = agent_profit + $4 ' +
                'WHERE id = $5',
    [total_amount, gross_profit, play_info.cash_out, agent_profit, userId], function (err, res) {
        if (err) {
            if (callback) { return callback(err); }
        }

        assert(res.rowCount < 2);

        lib.log('success', 'add_satoshis - [end]   user_id:' + userId + '   amount:' + amount + '   extra_bet:' + extraBet + '   agent_profit:' + agent_profit);
        if (callback) { return callback(null); }
    });
}

/**
 * Plus Balance-Satoshis to user with username in users table
 * @modified Bio
 */
function addSatoshisWithUsername (client, username, amount, extraBet, callback) {
    lib.log('success', 'add_satoshis_with_username - [begin]   username:' + username + '   amount:' + amount + '   extra_bet:' + extraBet);
    var total_amount = amount + extraBet;
    var sql = 'UPDATE users ' +
                'SET balance_satoshis = balance_satoshis + $1, ' +
                'agent_profit = agent_profit + $1 ' +
                // ', gross_profit = gross_profit + $1 + $2 ' +
                // ', net_profit = net_profit + $1 + $2 ' +
                'WHERE username = $2';
    client.query(sql, [total_amount, username], function (err, res) {
        if (err) return callback(err);
        // assert(res.rowCount === 1);
        lib.log('success', 'add_satoshis_with_username - [end]   username:' + username + '   amount:' + amount + '   extra_bet:' + extraBet);
        callback(null);
    });
}

/**
 * the function that is called when the a game finishes
 * update cash_out by play_id
 * @param userId        integer
 * @param playId        integer
 * @param amount        integer
 * @param extraBet      integer
 * @param extraSuccess  true/false
 * @param demo          true/false
 * @param extrabet_multiplier
 * @param callback
 */
exports.cashOut = function (userId, playId, amount, extraBet, extraSuccess, demo, extrabet_multiplier, callback) {
    lib.log('success', 'db.cashout - [begin]   play_id:' + playId +
        '   user_id:' + userId +
        '   amount:' + amount +
        '   extra_bet:' + extraBet +
        '   extra_success:' + extraSuccess +
        '   demo:' + demo +
        '   extrabet_multiplier:' + extrabet_multiplier);
    assert(typeof userId === 'number');
    assert(typeof playId === 'number');
    assert(typeof amount === 'number');
    assert(typeof extraBet === 'number');
    assert(typeof callback === 'function');

    getClient(function (client, callback) {
        var cash_out = amount + extraBet;
        client.query(
            'UPDATE plays SET cash_out = $1 WHERE id = $2 AND cash_out = 0',
            [cash_out, playId], function (err, result) {
                if (err) { return callback(err); }

                if (result.rowCount !== 1) {
                    console.error('G: Double cashout? ',
                        'User: ', userId, ' play: ', playId, ' amount: ', amount,
                        ' got: ', result.rowCount);

                    return callback(new Error('Double cashout'));
                }
                lib.log('success', 'db.cashout - [end]   user_id:' + userId + '   play_id:' + playId);
                return callback(null);
            });
    }, callback);
};

// callback called with (err, { crashPoint: , hash: })

/**
 * Create Game with game_hashes
 * @param gameId
 * @param callback
 */
exports.createGame = function (gameId, callback) {
    lib.log('info', 'db.create_game - [begin]   game_id:' + gameId);
    console.log('info', 'db.create_game - [begin]   game_id:' + gameId);
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    query('SELECT hash FROM game_hashes WHERE game_id = $1', [gameId], function (err, results) {
        if (err) return callback(err);

        if (results.rows.length !== 1) {
            console.error('G: Error: Not find game hash: ', gameId);
            return callback('NO_GAME_HASH');
        }

        // get the interval_status from common table.
        // if the interval_status is not 0, admin can use the intevals that the admin sets.
        query("SELECT strvalue FROM common WHERE strkey = 'interval_status'", function (err, interval_status) {
            if (err) return callback(err);

            assertIntervals(function (bIntervals) {
                if (interval_status.rows.length == 0 || interval_status.rows[0].strvalue == 0 || bIntervals == false) {
                    var hash = results.rows[0].hash;
                    var gameCrash = lib.crashPointFromHash(hash);
                    assert(lib.isInt(gameCrash));

                    console.log('db.creategame - game insert original mode [begin]    game_id:' + gameId + '   game_crash:' + gameCrash);
                    lib.log('info', 'db.creategame - game insert original mode [begin]    game_id:' + gameId + '   game_crash:' + gameCrash);
                    query('INSERT INTO games(id, game_crash) VALUES($1, $2)', [gameId, gameCrash], function (err) {
                        if (err) return callback(err);

                        console.log('db.creategame - game insert original mode [end]    game_id:' + gameId + '   game_crash:' + gameCrash);
                        lib.log('info', 'db.creategame - game insert original mode [end]    game_id:' + gameId + '   game_crash:' + gameCrash);

                        var pp = results.rows[0].hash;

                        return callback(null, { crashPoint: gameCrash, hash: hash });
                    });
                } else if (interval_status.rows.length != 0 && interval_status.rows[0].strvalue == 1) {
                    if (bIntervals === false) {
                        return callback('\nASSERTION ERROR OCCURED.');
                    }

                    hash = results.rows[0].hash;
                    var gameCrash;
                    getIntervals(function (intervals) {
                        if (intervals == null) {
                            console.error('Error occured when fetching records.');
                            lib.log('error', 'db.create_game - error occured when fetching records.');
                            return callback('Error occured when fetching records.');
                        }

                        gameCrash = lib.crashPointFromHash(hash, intervals); // set the crash point with intervals

                        assert(lib.isInt(gameCrash));

                        console.log('db.creategame - game insert manual mode [begin]    game_id:' + gameId + '   game_crash:' + gameCrash);
                        lib.log('info', 'db.creategame - game insert manual mode [begin]    game_id:' + gameId + '   game_crash:' + gameCrash);

                        query('INSERT INTO games(id, game_crash) VALUES($1, $2)',
                            [gameId, gameCrash], function (err) {
                                if (err) return callback(err);

                                console.log('db.creategame - game insert manual mode [end]    game_id:' + gameId + '   game_crash:' + gameCrash);
                                lib.log('info', 'db.creategame - game insert manual mode [end]    game_id:' + gameId + '   game_crash:' + gameCrash);

                                console.log('success', 'db.create_game - [end]   game_id:' + gameId + '   game_crash:' + gameCrash);
                                return callback(null, {
                                    crashPoint: gameCrash,
                                    hash: hash
                                });
                            });
                    });
                }
            });
        });
    });
};

function assertIntervals (callback) {
    lib.log('info', 'db.assert_intervals - [begin]');
    console.log('info', 'db.assert_intervals - [begin]');
    query('SELECT * FROM intervals ORDER BY interval_start ASC', [], function (err, results) {
        if (err) return callback(false);
        if (results.rowCount == 0) return callback(false);

        var old = results.rows[0].interval_start;
        var sum = 0;

        results.rows.forEach(function (interval) {
            if (old != interval.interval_start) {
                lib.log('error', 'db.assert_intervals - the intervals should be not be overlapped or not allowed');
                console.error('ASSERTION ERROR: The intervals should not be overlapped or no gaps are not allowed.');
                return callback(false);
            }
            sum += interval.percentage;
            old = interval.interval_end;
        });

        if (sum != 10000) {
            lib.log('error', 'db.assert_intervals - the totla of percentage of intervals is not 100 %');
            console.error('ASSERTION ERROR: The total of percentages of intervals is not 100%.');
            return callback(false);
        }

        lib.log('info', 'db.assert_intervals - [end]');
        console.log('info', 'db.assert_intervals - [end]');
        return callback(true);
    });
}

function getIntervals (callback) {
    // var retVal = [];
    lib.log('success', 'db.get_intervals - [begin]');
    query('SELECT * FROM intervals ORDER BY interval_start ASC', [], function (err, results) {
    // console.log('type of results.rows =>' + typeof(results.rows));
    // console.log('type of results.rows[0] =>' + typeof(results.rows[0]));
    // console.log('type of results.rows[0].percentage =>' + typeof(results.rows[0].percentage));
        if (err) {
            lib.log('error', 'error occured when fetching records from intervals table.');
            console.error('Error occured when fetching records from intervals table.');
            callback(null);
        }
        lib.log('success', 'db.get_intervals - [end]');
        callback(results.rows);
    });
}

/**
 * Bio
 * calc bankroll from db, bankroll = total_deposit - total_withdrawal - total_user_balances
 */
exports.getBankroll = function (callback) {
    lib.log('success', 'db.get_bankroll - [begin]');
    query("SELECT strvalue FROM common WHERE strkey = 'add_gaming_pool'", function (err, addgp) {
        if (err) throw err;
        var add_gaming_pool = 0;
        if (addgp.rows.length == 1) {
            add_gaming_pool = addgp.rows[0]['strvalue'];
        }
        add_gaming_pool = parseInt(add_gaming_pool);
        lib.log('success', 'db.get_bankroll -    add_gaming_pool:' + add_gaming_pool);

        query('SELECT (' +
            '(SELECT COALESCE(SUM(amount),0) FROM fundings) - ' +
            '(SELECT COALESCE(SUM(balance_satoshis), 0) FROM users WHERE demo=false)) AS profit',
        function (err, results) {
            if (err) return callback(err);

            assert(results.rows.length === 1);
            var profit = results.rows[0].profit;
            assert(typeof profit === 'number');
            lib.log('success', 'db.get_bankroll -    profit:' + profit);

            lib.log('success', 'db.get_bankroll - [end]   gaming_pool:' + parseInt(profit + add_gaming_pool));
            return callback(null, profit + add_gaming_pool);

            // var min = 1e8;
            // return callback(null, Math.max(min, profit));
        }
        );
    });
};

/**
 * Bio
 * calc fake money : balances from all demo users
 */
exports.getFakePool = function (callback) {
    lib.log('success', 'db.get_fake_pool - [begin]');
    var sql = "SELECT strvalue FROM common WHERE strkey='deposit_fakepool'";
    query(sql, function (e, r) {
        if (e) return callback(null, 0);

        var deposit_fakepool = 0;
        if (r.rowCount == 1) deposit_fakepool = r.rows[0].strvalue;
        deposit_fakepool = parseInt(deposit_fakepool);
        if (isNaN(deposit_fakepool)) deposit_fakepool = 0;

        lib.log('success', 'db.get_fake_pool -   deposit_fakepool:' + deposit_fakepool);

        query('SELECT $1 - COALESCE(SUM(balance_satoshis), 0) AS profit FROM users WHERE demo=true', [deposit_fakepool], function (err, results) {
            if (err) return callback(err);

            assert(results.rows.length === 1);

            var profit = results.rows[0].profit;
            assert(typeof profit === 'number');
            lib.log('success', 'db.get_fake_pool -   profit:' + profit);
            lib.log('success', 'db.get_fake_pool - [end]');
            callback(null, profit);
        });
    });
};

exports.getGameHistory = function (callback) {
    // 222 var sql =
    //     'SELECT games.id game_id, game_crash, created, ' +
    //     '(SELECT hash FROM game_hashes WHERE game_id = games.id), ' +
    // '(SELECT to_json(array_agg(to_json(pv))) ' +
    // 'FROM ( ' +
    // 'SELECT users.username, plays.bet, (100 * plays.cash_out / plays.bet) AS stopped_at, ' +
    // 'plays.extra_bet, profits.profit_for_player AS profit ' +
    // 'FROM plays ' +
    // 'JOIN users ON user_id = users.id ' +
    // 'JOIN profits ON profits.play_id = plays.id ' +
    // 'WHERE plays.game_id = games.id ' +
    // ') pv) player_info ' +
    // 'FROM games ' +
    // 'WHERE games.ended = true ' +
    // 'ORDER BY games.id DESC LIMIT 20';

        /*
    var sql =
        'SELECT games.id game_id, game_crash, created, ' +
        '(SELECT hash FROM game_hashes WHERE game_id = games.id), ' +
        '(SELECT to_json(array_agg(to_json(pv))) ' +
        'FROM ( ' +
        'SELECT users.username, plays.bet, (100 * plays.cash_out / plays.bet) AS stopped_at, ' +
        'plays.extra_bet ' +
        'FROM plays ' +
        'JOIN users ON user_id = users.id ' +
        'WHERE plays.game_id = games.id ' +
        ') pv) player_info ' +
        'FROM games ' +
        'WHERE games.ended = true ' +
        'ORDER BY games.id DESC LIMIT 20';

    query(sql, function (err, data) {
        if (err) throw err;

        data.rows.forEach(function (row) {
            // oldInfo is like: [{"username":"USER","bet":satoshis, ,..}, ..]
            var oldInfo = row.player_info || [];
            var newInfo = row.player_info = {};

            oldInfo.forEach(function (play) {
                newInfo[play.username] = {
                    bet: play.bet,
                    extraBet: play.extra_bet,
                    stopped_at: play.stopped_at
                };
            });
        });

        callback(null, data.rows);
    });
    */
    callback(null, []);
};

/**
 * Get All Players' Profit Per Game (play page)
 * @author  Bio
 * @param callback
 */
exports.getAllPlayerProfitPerGame = function (gameId, callback) {
    var sql = 'SELECT users.username, profits.*, plays.extra_bet ' +
                'FROM profits ' +
                'LEFT JOIN users ON profits.user_id = users.id ' +
                'LEFT JOIN plays on plays.id = profits.play_id ' +
                'WHERE profits.game_id = $1 ' +
                'ORDER BY plays.created DESC';

    query(sql, [gameId], function (err, result) {
        result = result.rows;
        var allPlayerProfitPerGame = {};
        for (var i = 0; i < result.length; i++) {
            allPlayerProfitPerGame[result[i]['username']] = result[i]['profit_for_player'];
        }

        return callback(null, allPlayerProfitPerGame);
    });
};

/**
 * Get a record of plays table by id
 * @author Bio
 * @param callback
 */
exports.getPlayInfoFromPlayId = function (playId, callback) {
    var sql = 'SELECT * FROM plays WHERE id = ' + playId;
    query(sql, function (err, result) {
        result = result.rows;
        if (result.length != 0) { return callback(null, result[0]); }
        return callback(null);
    });
};

/**
 * Get a maximum profit percent for bankroll
 * @author Bio
 * @param callback
 */
exports.getMaxProfit = function (callback) {
    lib.log('success', 'db.get_max_profit - [begin]');
    var sql = "SELECT strvalue FROM common WHERE strkey='max_profit'";
    query(sql, function (e, r) {
        if (e) { return callback(e); }

        if (r.rowCount == 1) {
            lib.log('success', 'db.get_max_profit - [end]   max_profit:' + r.rows[0].strvalue);
            return callback(null, r.rows[0].strvalue);
        }
        lib.log('success', 'db.get_max_profit - [end]   max_profit_default:3');
        return callback(null, 3);
    });
};

/**
 * check user's parent can become agent
 * @author Bio
 * @param userId : user's id who is betting
 * @param callback
 */
exports.checkCanBeAgent = function (userId, callback) {
    // load basic values
    lib.log('success', 'db.check_can_be_agent - [begin]   user_id:' + userId);
    var sql = 'SELECT ' +
        '(SELECT ref_id FROM users WHERE id=$1 LIMIT 1), ' +
        '(SELECT userclass FROM users WHERE id=$1 LIMIT 1), ' +
        "(SELECT strvalue AS to_be_agent_deposit_multiplier FROM common WHERE strkey='to_be_agent_deposit_multiplier'), " +
        "(SELECT strvalue AS to_be_agent_client_count FROM common WHERE strkey='to_be_agent_client_count')";
    query(sql, [userId], function (err, result_1) {
        if (err) return callback(err);
        if (result_1.rowCount != 1) return callback('ERROR_GET_AGENT_1');

        var ref_id = result_1.rows[0].ref_id; // user's parent <username>
        var userclass = result_1.rows[0].userclass; // user's parent <username>
        var to_be_agent_deposit_multiplier = result_1.rows[0].to_be_agent_deposit_multiplier; // if ref_id want to become agent, his parent, that is , user has to bet - <to_be_agent_deposit_multiplier> times of first user's first deposit amount
        var to_be_agent_client_count = result_1.rows[0].to_be_agent_client_count; // if ref_id want to becom agent, <to_be_agent_client_count> children have to bet <to_be_agent_deposit_multiplier> times of first user's first deposit amount

        lib.log('success', 'db.check_can_be_agent -   user_id:' + userId +
            '   ref_id:' + ref_id +
            '   userclass:' + userclass +
            '   to_be_agent_deposit_multiplier:' + to_be_agent_deposit_multiplier +
            '   to_be_agent_client_count:' + to_be_agent_client_count);
        // that is number of player need to be agent condition
        if (ref_id == null || userclass == 'agent' || userclass == 'master_ib') {
            // console.log("agent check : no need check.");
            lib.log('success', 'db.check_can_be_agent -   user_id:' + userId + '   no_need_check');
            return callback(null, 'NO_NEED_CHECK');
        } // if this user is normal user (who was not introduced to this site, and he found this game site by himeself.), no need to check about his parent

        to_be_agent_deposit_multiplier = parseFloat(to_be_agent_deposit_multiplier);
        to_be_agent_client_count = parseInt(to_be_agent_client_count);

        if (isNaN(to_be_agent_deposit_multiplier)) to_be_agent_deposit_multiplier = 0;
        if (isNaN(to_be_agent_client_count)) to_be_agent_client_count = 1;

        if (to_be_agent_deposit_multiplier != 0) {
            sql = 'select count(*) as clients_cnt from users where \n' +
                '(select sum(plays.bet) + sum(plays.extra_bet) + sum(plays.range_bet_amount) from plays where plays.user_id=users.id GROUP BY plays.user_id) \n' +
                '>= \n' +
                '(SELECT fundings.amount FROM fundings WHERE fundings.user_id=users.id ORDER BY fundings.created LIMIT 1) * ' + to_be_agent_deposit_multiplier + '\n' +
                'AND\n' +
                '(SELECT fundings.amount FROM fundings WHERE fundings.user_id=users.id ORDER BY fundings.created LIMIT 1) != 0\n' +
                'AND\n' +
                "users.ref_id='" + ref_id + "'"; // calculate the number of players who had deposit of upper condition
        } else {
            sql = "select count(*) as clients_cnt from users where ref_id='" + ref_id + "'"; // calculate the number of players who had deposit of upper condition
        }

        query(sql, function (e, r) {
            if (e) { return callback(e); }

            if (r.rowCount != 1) {
                lib.log('success', 'db.check_can_be_agent -   user_id:' + userId + '   can_be_agent_error');
                return callback('CAN_BE_AGENT_ERROR');
            }

            console.log('db.check_can_be_agent - info -   ref_id:' + ref_id + '   client : ' + r.rows[0].clients_cnt);
            lib.log('info', 'db.check_can_be_agent -   ref_id:' + ref_id + '   client : ' + r.rows[0].clients_cnt);

            var nCntClients = parseInt(r.rows[0].clients_cnt);
            if (nCntClients >= to_be_agent_client_count - 1) {
                // register ref-id as parent
                query("UPDATE users SET userclass = 'agent' WHERE username=$1 RETURNING path;", [ref_id], function (err, result_2) {
                    if (err) return callback(err);
                    console.log('[', ref_id, '] became agent.');

                    // all children, grand-children, grand-grand-children to third layer have to be set as parent field
                    // lib.log('success', 'db.check_can_be_agent -   user_id:' + userId + '   agent_path:' + agent_path);

                    var agent_path = result_2.rows[0].path;
                    var sql = "UPDATE users SET parent1=$1 WHERE path like '" + agent_path + "___'";
                    query(sql, [ref_id], function (err) {
                        if (err) return callback(err);
                        sql = "UPDATE users SET parent2=$1 WHERE path like '" + agent_path + "______'";
                        query(sql, [ref_id], function (err) {
                            if (err) return callback(err);
                            sql = "UPDATE users SET parent3=$1 WHERE path like '" + agent_path + "_________'";
                            query(sql, [ref_id], function (err) {
                                if (err) return callback(err);
                                return callback(null);
                            });
                        });
                    });
                });
            } else {
                console.log('check agent : client count not enough.  now:' + nCntClients);
                lib.log('success', 'db.check_can_be_agent -   user_id:' + userId + '   client_count_not_enough.  now:' + nCntClients);
                return callback(null);
            }
        });
    });
};

/**
 * check use's first deposit can be transfed to user's parent
 * @author Bio
 * @param userId : user's id who is betting
 * @param palyId : play id of game
 * @param callback
 */
exports.checkFirstDepositFee_ = function (userId, playId, callback) {
    lib.log('success', 'db.check_first_deposit_fee -   user_id:' + userId + '   play_id:' + playId);
    query('SELECT ' +
        '(SELECT did_ref_deposit FROM users WHERE id=$1), ' +
        "(SELECT strvalue AS first_deposit_percent FROM common WHERE strkey='first_deposit_percent'), " +
        "(SELECT strvalue AS first_deposit_multiplier FROM common WHERE strkey='first_deposit_multiplier'), " +
        '(SELECT amount AS first_deposit_amount FROM fundings WHERE user_id=$1 ORDER BY created LIMIT 1), ' +
        '(SELECT ref_id FROM users WHERE id=$1)', [userId], function (err, result_1) {
        if (err) return callback(err);

        if (result_1.rowCount != 1) return callback('ERROR_DID_REF_DEPOSIT');

        var did_ref_deposit = result_1.rows[0].did_ref_deposit; // bool : if this user gave <first_deposit_fee> to parent
        var first_deposit_percent = result_1.rows[0].first_deposit_percent; // pecent for first deposit of user
        var first_deposit_multiplier = result_1.rows[0].first_deposit_multiplier; // multiplier
        var first_deposit_amount = result_1.rows[0].first_deposit_amount;// first deposit amount : if user's want to have <fist deposit fee> from user, user have to bet <first_deposit_multiplier> * <first_deposit_percent percent of first_deposit_amount>
        var ref_id = result_1.rows[0].ref_id; // user's parent

        lib.log('success', 'db.check_first_deposit_fee -   user_id:' + userId + '   play_id:' + playId +
                '   did_ref_deposit:' + did_ref_deposit +
                '   first_deposit_percent:' + first_deposit_percent +
                '   first_deposit_multiplier:' + first_deposit_multiplier +
                '   first_deposit_amount:' + first_deposit_amount +
                '   ref_id:' + ref_id);

        if (ref_id == null || ref_id == undefined) {
            lib.log('success', 'db.check_first_deposit_fee - no_ref_id   user_id:' + userId + '   play_id:' + playId);
            return callback(null, {msg: 'NO_REF_ID'});
        }
        if (did_ref_deposit == true) {
            lib.log('success', 'db.check_first_deposit_fee - no_need_ref_deposit   user_id:' + userId + '   play_id:' + playId);
            return callback(null, {msg: 'NO_NEED_REF_DEPOSIT'});
        }

        first_deposit_percent = parseFloat(first_deposit_percent);
        first_deposit_multiplier = parseFloat(first_deposit_multiplier);
        first_deposit_amount = parseFloat(first_deposit_amount);

        if (isNaN(first_deposit_amount) || first_deposit_amount <= 0) {
            lib.log('success', 'db.check_first_deposit_fee - not_deposit   user_id:' + userId + '   play_id:' + playId);
            return callback(null, {msg: 'NOT_DEPOSIT'});// user didn't depoist yet
        }

        if (isNaN(first_deposit_percent)) first_deposit_percent = 0;
        if (isNaN(first_deposit_multiplier)) first_deposit_multiplier = 0;

        var fAvailableFee = first_deposit_amount * first_deposit_percent / 100; // real fee
        var fNeededBet = fAvailableFee * first_deposit_multiplier; // bet amount to make <first deposit fee>

        lib.log('success', 'db.check_first_deposit_fee - user_id:' + userId + '   play_id' + playId + '    available_fee:' + fAvailableFee + '   needed_bet:' + fNeededBet);

        query('select count(*) as cnt_fdf from users where \n' +
            '(select sum(plays.bet) + sum(plays.extra_bet) + sum(plays.range_bet_amount) from plays where plays.user_id=users.id GROUP BY plays.user_id) > $1\n' +
            'AND \n' +
            'users.id=$2', [fNeededBet, userId], function (err, cnt_fdf) {
            // check condition
            if (err) return callback(err);
            if (cnt_fdf.rowCount != 1) {
                lib.log('error', 'db.check_first_deposit_fee - error_cnt_fdf   user_id:' + userId + '   play_id:' + playId);
                return callback('ERROR_CNT_FDF');
            }
            cnt_fdf = cnt_fdf.rows[0].cnt_fdf;
            cnt_fdf = parseInt(cnt_fdf);

            if (cnt_fdf != 1) {
                lib.log('success', 'db.check_first_deposit_fee - not_bet_available   user_id:' + userId + '   play_id:' + playId);
                return callback(null, {msg: 'NOT_BET_AVAILABLE'});
            }

            // make deposit fee
            query('UPDATE users SET did_ref_deposit=true WHERE id=$1', [userId], function (err) {
                if (err) return callback(err);
                query('UPDATE plays SET first_deposit_profit = $1 WHERE id=$2', [Math.round(fAvailableFee), playId], function (err) {
                    if (err) return callback(err);

                    query('UPDATE users SET balance_satoshis = balance_satoshis + $1, first_deposit_profit = first_deposit_profit + $1 ' +
                        'WHERE lower(username) = lower($2)', [Math.round(fAvailableFee), ref_id], function (err) {
                        if (err) return callback(err);

                        query("UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE username = 'madabit'", [fAvailableFee], function (err, data) {
                            if (err) return callback(err);

                            console.log('[', ref_id, '] got first deposit fee.');
                            lib.log('success', 'db.check_first_deposit_fee - got_first_deposit_fee   user_id:' + userId + '   play_id:' + playId);

                            return callback(null, {msg: 'GAVE_FEE', toAccount: ref_id, fAvailableFee: fAvailableFee / 100});
                        });
                    });
                });
            });
        });
    });
};

/**
 * check user's new deposit
 * @author Bio
 * @since 2018.6.25
 * @param userId : user's id who is betting
 * @param palyId : play id of game
 * @param callback
 * Funding Bonus System changed.
 * Whenever players deposit, their parents receive first deposit fee
 */
exports.checkFirstDepositFee = function (userId, playId, callback) {
    lib.log('success', 'db.check_first_deposit_fee -   user_id:' + userId + '   play_id:' + playId);
    query('SELECT ' +
        "(SELECT strvalue AS first_deposit_percent FROM common WHERE strkey='first_deposit_percent'), " +
        '(SELECT ref_id FROM users WHERE id=$1)', [userId], function (err, result_1) {
        if (err) return callback(err);

        if (result_1.rowCount != 1) return callback('ERROR_DID_REF_DEPOSIT');
        var first_deposit_percent = result_1.rows[0].first_deposit_percent; // pecent for first deposit of user
        var ref_id = result_1.rows[0].ref_id; // user's parent

        lib.log('success', 'db.check_first_deposit_fee -   user_id:' + userId + '   play_id:' + playId +
            '   first_deposit_percent:' + first_deposit_percent +
            '   ref_id:' + ref_id);

        if (ref_id == null || ref_id == undefined) {
            lib.log('success', 'db.check_first_deposit_fee - no_ref_id   user_id:' + userId + '   play_id:' + playId);
            return callback(null, {msg: 'NO_REF_ID'});
        }

        first_deposit_percent = parseFloat(first_deposit_percent);

        if (isNaN(first_deposit_percent))
            first_deposit_percent = 0;

        var sql_new_deposit = 'SELECT * FROM fundings WHERE user_id = $1 AND gave_bonus = false AND amount > 0';
        query(sql_new_deposit, [userId], function (err, result) {
            if (err) {
                console.log('error', 'db.checkNewDeposit - user_id:' + userId);
                lib.log('error', 'db.checkNewDeposit - user_id:' + userId);
            }

            result = result.rows;
            var tasks = [];
            result.forEach(function (fundingInfo) {
                tasks.push(function (callback) {
                    var fAvailableFee = fundingInfo.amount * first_deposit_percent / 100;
                    query('UPDATE plays SET first_deposit_profit = first_deposit_profit + $1 WHERE id=$2', [Math.round(fAvailableFee), playId], function (err) {
                        if (err) return callback(err);
                        query('UPDATE users SET balance_satoshis = balance_satoshis + $1, first_deposit_profit = first_deposit_profit + $1 ' +
                            'WHERE lower(username) = lower($2)', [Math.round(fAvailableFee), ref_id], function (err) {
                            if (err) return callback(err);
                            query("UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE username = 'madabit'", [fAvailableFee], function (err) {
                                if (err) return callback(err);
                                query('UPDATE fundings SET gave_bonus = true WHERE id = $1', [fundingInfo.id], function (err) {
                                    if(err)
                                        return callback(err);
                                    var message_to_user = 'login_bonus:' + (fAvailableFee / 100);
                                    query('SELECT * FROM users WHERE username = $1', [ref_id], function(err, result) {
                                        if(err || result.rows.length == 0)
                                            return callback(err);
                                        var parent_id = result.rows[0].id;
                                        query('INSERT INTO supports (user_id, message_to_user, read, reply_check) VALUES ($1, $2, true, false)', [parent_id, message_to_user], function (err) {
                                            if (err) return callback(err);
                                            return callback(null, {
                                                msg: 'GAVE_FEE',
                                                toAccount: ref_id,
                                                fAvailableFee: fAvailableFee});
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });

            async.series(tasks, function (err, results) {
                if (err) {
                    return callback(err);
                }
                var totalFee = 0;
                var toAccount = '';
                for (var i = 0; i < results.length; i++) {
                    if (results[i]['msg'] == 'GAVE_FEE') {
                        toAccount = results[i].toAccount;
                        totalFee += results[i].fAvailableFee;
                    }
                }
                if (totalFee !== 0) {
                    return callback(null, {
                        msg: 'GAVE_FEE',
                        toAccount: toAccount,
                        fAvailableFee: totalFee / 100
                    });
                }
                return callback(null);
            });
        });
    });
};

/**
 * init login bonus system to sunday
 * @author Bio
 * @param userId : user's id who is betting
 * @param callback
 */
function initLoginBonusCycle (userId, time_zone, callback) {
    lib.log('success', 'db.init_login_bonus_cycle - [begin]    user_id:' + userId + '   time_zone:' + time_zone);
    var sql = 'SELECT ' +
        "(SELECT (created at time zone '" + time_zone + "')::DATE " +
        'FROM plays ' +
        'WHERE login_bonus_count=(SELECT MAX(login_bonus_count) FROM plays WHERE user_id=$1) AND user_id=$1 ORDER BY created DESC LIMIT 1) AS last_got,' +
        "(SELECT (now() at time zone '" + time_zone + "')::DATE - EXTRACT(DOW from now() at time zone '" + time_zone + "')::INTEGER) AS last_sunday," +
        "(SELECT (created at time zone '" + time_zone + "')::DATE " +
        'FROM plays ' +
        'WHERE login_bonus_count=(SELECT MAX(login_bonus_count) FROM plays WHERE user_id=$1) AND user_id=$1 ORDER BY created DESC LIMIT 1) ' +
        ' < ' +
        "(SELECT (now() at time zone '" + time_zone + "')::DATE - EXTRACT(DOW from now() at time zone '" + time_zone + "')::INTEGER) " +
        'AS b_is_first_login';

    query(sql, [userId], function (err, result) {
        if (err) return callback(err);
        if (result.rowCount == 0) {
            console.log('initLoginBonusCycle : pure_state');
            lib.log('success', 'db.init_login_bonus_cycle - pure_state    user_id:' + userId);
            // lib.log("db", "initLoginBonusCycle : pure_state");
            return callback(null, 'PURE_STATE');
        }

        var last_got = result.rows[0].last_got;
        var last_sunday = result.rows[0].last_sunday;
        var isFirstLogin = result.rows[0].b_is_first_login;

        lib.log('success', 'db.init_login_bonus_cycle - last_got:' + last_got + '   user_id:' + userId);
        lib.log('success', 'db.init_login_bonus_cycle - last_sunday:' + last_sunday + '   user_id:' + userId);
        lib.log('success', 'db.init_login_bonus_cycle - is_first_login:' + isFirstLogin + '   user_id:' + userId);

        last_got = last_got.toLocaleString();
        last_sunday = last_sunday.toLocaleString();

        if (isFirstLogin == true) {
            console.log('time_zone :', time_zone, '    last_got :', last_got, '   last_sunday :', last_sunday, '   last_got < last_sunday');
            // lib.log("db", "time_zone :", time_zone, "    last_got :", last_got, "   last_sunday :",last_sunday, "   last_got < last_sunday");

            /* client. */query('UPDATE plays SET login_bonus_count=0 WHERE user_id=$1', [userId], function (err) {
                if (err) {
                    // console.log("initLoginBonusCycle : db error 2");
                    return callback(err);
                }

                lib.log('success', 'db.init_login_bonus_cycle - initialized    user_id ' + userId);
                console.log('initLoginBonusCycle : INITIALIZED');
                // lib.log("db", "initLoginBonusCycle : INITIALIZED");
                return callback(null, 'INITIALIZED');
            });
        } else {
            lib.log('success', 'db.init_login_bonus_cycle - [end] no_need_init  is_first_login:' + isFirstLogin + '   user_id:' + userId);
            console.log('time_zone :', time_zone, '    last_got :', last_got, '   last_sunday :', last_sunday, '   last_got >= last_sunday');
            // lib.log("db", "time_zone :" + time_zone + "    last_got :" + last_got + "   last_sunday : " + last_sunday + "   last_got >= last_sunday");
            return callback(null, 'NO_NEED_INIT');
        }
    });
}

/**
 * check if user can get login bonus today
 * @author Bio
 * @param userId : user's id who is betting
 * @param callback
 */
exports.checkLoginBonus = function (userId, playId, time_zone, callback) {
    lib.log('success', 'db.check_login_bonus - [begin]   user_id:' + userId + '   play_id:' + playId + '   time_zone:' + time_zone);
    initLoginBonusCycle(userId, time_zone, function (err, result_0) {
        if (err) { return callback(err); }

        query('SELECT ' +
            '(SELECT max(login_bonus_count) AS max_play FROM plays WHERE user_id=$1), ' +
            '(SELECT max(id) AS max_bonus FROM login_bonus), ' +
            "(SELECT strvalue AS lb_bet FROM common WHERE strkey='login_bonus_bet')", [userId], function (err, result_1) {
            if (err) { return callback(err); }

            if (result_1.rowCount != 1) {
                console.log('checkLoginBonus : error LB 1');
                return callback('ERROR_LB_1');
            }
            var max_play = result_1.rows[0].max_play; // to which login bonus user had
            var max_bonus = result_1.rows[0].max_bonus; // number of login bonuses
            var login_bonus_bet = result_1.rows[0].lb_bet; // multiplier needed to get login bonus

            max_play = parseInt(max_play);
            max_bonus = parseInt(max_bonus);
            login_bonus_bet = parseInt(login_bonus_bet);

            if (isNaN(max_play)) max_play = 0;
            if (isNaN(max_bonus)) max_bonus = 0;
            if (isNaN(login_bonus_bet)) login_bonus_bet = 1;

            lib.log('success', 'db.check_login_bonus -  max_play:' + max_play + '   max_bonus:' + max_bonus + '   login_bonus_bet:' + login_bonus_bet + '   user_id:' + userId + '   play_id:' + playId);

            if (max_play >= max_bonus) {
                lib.log('success', 'db.check_login_bonus -  bonus list limit.   user_id:' + userId + '   play_id:' + playId);
                console.log('check login bonus : bonus list limit.');
                // lib.log("db", "check login bonus : bonus list limit.");
                return callback(null, {msg: 'BONUS_LIST_LIMIT'});
            }

            query('SELECT ' +
                '(SELECT sum(bet) + sum(extra_bet) + sum(range_bet_amount) AS today_bet ' +
                'FROM plays ' +
                "WHERE date(created at time zone '" + time_zone + "')=date(now() at time zone '" + time_zone + "') AND user_id=$1), " +
                '(SELECT bonus AS today_bonus FROM login_bonus WHERE id=$2), ' +
                "(SELECT date(now() at time zone '" + time_zone + "')=date(created at time zone '" + time_zone + "') AS is_today " +
                'FROM plays ' +
                'WHERE login_bonus_count=$3 AND user_id=$1 ORDER BY created DESC LIMIT 1)', [userId, max_play + 1, max_play], function (err, result_2) {
                if (err) {
                    lib.log('error', 'db.check_login_bonus -  db error 2  user_id:' + userId + '   play_id:' + playId);
                    console.log('checkLoginBonus : db error 2');
                    return callback(err);
                }

                if (result_2.rowCount != 1) {
                    lib.log('error', 'db.check_login_bonus -  LB 2  user_id:' + userId + '   play_id:' + playId);
                    console.log('checkLoginBonus : ');
                    return callback('ERROR_LB_2');
                }

                var today_bet = result_2.rows[0].today_bet; // toatl bet amount that user made bet today
                var today_bonus = result_2.rows[0].today_bonus; // bonus amount which user can get when he will make necessary bet amount
                var is_today = result_2.rows[0].is_today; // check whether user got <login bonus> today

                if (max_play != 0 && is_today == true) {
                    lib.log('success', 'db.check_login_bonus -  waiting next day   user_id:' + userId + '   play_id:' + playId);
                    console.log('check login bonus : waiting next day : userId :', userId);
                    // lib.log("db", "check login bonus : waiting next day : userId :" + userId);
                    return callback(null, {msg: 'WAITING_NEXT_DAY'});
                }

                if (today_bet === null) today_bet = 0;
                today_bet = parseInt(today_bet);
                if (isNaN(today_bet)) {
                    lib.log('error', 'db.check_login_bonus -  LB 3  user_id:' + userId + '   play_id:' + playId);
                    console.log('checkLoginBonus : ');
                    return callback('ERROR_LB_3');
                }

                today_bonus = parseInt(today_bonus) * 100;

                // check bet amount > critaria
                if (today_bet < login_bonus_bet * today_bonus) {
                    lib.log('error', 'db.check_login_bonus -  today_bet is not enough  user_id:' + userId + '   play_id:' + playId);
                    console.log('checkLoginBonus : today_bet is not enough');
                    // lib.log("db", "checkLoginBonus : today_bet is not enough");
                    return callback(null, {msg: 'NOT_ENOUGH_BET'});
                }

                // make login bonus
                query('UPDATE users SET balance_satoshis = balance_satoshis + $1,play_times_profit=play_times_profit+$1 WHERE id = $2', [today_bonus, userId], function (err) {
                    if (err) {
                        lib.log('error', 'db.check_login_bonus -  db error 3  user_id:' + userId + '   play_id:' + playId);
                        console.log('checkLoginBonus : db error 3');
                        return callback(err);
                    }

                    // console.log("make login bonus : balance_satoshis + ", today_bonus, "userId:", userId, "bonus_count:", max_play + 1);
                    query('UPDATE plays SET play_times_profit=$1, login_bonus_count=$2 WHERE id=$3', [today_bonus, max_play + 1, playId], function (err) {
                        if (err) {
                            lib.log('error', 'db.check_login_bonus -  db error 4  user_id:' + userId + '   play_id:' + playId);
                            console.log('checkLoginBonus : db error 4');
                            return callback(err);
                        }

                        query("UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE username = 'madabit'", [today_bonus], function (err, data) {
                            if (err) {
                                lib.log('error', 'db.check_login_bonus -  db error 5  user_id:' + userId + '   play_id:' + playId);
                                console.log('checkLoginBonus : db error 5');
                                return callback(err);
                            }

                            lib.log('error', 'db.check_login_bonus -  [end]  bonus:' + today_bonus / 100 + '  user_id:' + userId + '   play_id:' + playId);
                            console.log('login bonus : PlayId :', playId, '     bonus :', today_bonus / 100, '    UserId:', userId);
                            // lib.log("db", "login bonus : PlayId :" + playId + "     bonus :" + today_bonus / 100 + "    UserId:" +  userId);
                            return callback(null, {msg: 'GAVE_BONUS', bonus: today_bonus});
                        });
                    });
                });
            });
        });
    });
};

/**
 * get multiplier value needed to extra bet , which means user can <extrabet_multiplier> times of <extra_bet_amount> when he succeed in extrabet
 * @author Bio
 * @param callback
 */
exports.getExtraBetMultiplier = function (callback) {
    lib.log('info', 'db.get_extra_bet_multiplier - [begin]');
    var sql = "SELECT strvalue FROM common WHERE strkey='extrabet_multiplier'";
    query(sql, function (e, r) {
        if (e) { return callback(e); }

        if (r.rowCount === 1) {
            return callback(null, r.rows[0].strvalue);
        }
        lib.log('info', 'db.get_extra_bet_multiplier - [end]   value:50');
        return callback(null, 50);
    });
};

/**
 * get total percent of agetn system
 * @author Bio
 * @param callback
 */
exports.getAgentSysFeePro = function (callback) {
    lib.log('success', 'db.get_agent_sysfee - [begin]');
    var sql = "SELECT sum(cast(strvalue as float)) AS agent_sys_fee_pro FROM common WHERE strkey LIKE 'agent_%'";
    query(sql, function (e, r) {
        if (e) { return callback(e); }

        if (r.rowCount == 1) {
            lib.log('success', 'db.get_agent_sysfee - [end]   sum_agent_percent:' + r.rows[0].agent_sys_fee_pro);
            return callback(null, r.rows[0].agent_sys_fee_pro);
        }
        lib.log('success', 'db.get_agent_sysfee - [end]   sum_agent_percent:0 default');
        return callback(null, 0);
    });
};

/**
 * init user's state
 * @author Bio
 * @param callback
 */
exports.clearPlaying = function (callback) {
    lib.log('success', 'db.clear_playing - [begin]');
    var sql = 'UPDATE users SET playing=false';
    query(sql, function (e) {
        if (e) return callback(e);
        lib.log('success', 'db.clear_playing - [end]');
        return callback(null);
    });
};

/**
 * calculate and save 8 percent of total bets
 * @author Bio
 * @param callback
 */

exports.saveInComeBets = function (bets, callback) {
    lib.log('success', 'db.save_in_ComeBet - [begin]   bets:' + bets);
    var sql = "SELECT * FROM common WHERE strkey = 'in_come_bets'";
    query(sql, function (err, res) {
        if (err) return callback(err);

        if (res.rowCount == 0) {
            sql = 'INSERT INTO common (strkey, strvalue) VALUES($1, $2) RETURNING *';
            query(sql, ['in_come_bets', bets], function (err, res) {
                if (err) return callback(err);

                if (res.rowCount == 1) {
                    console.log('success', 'db.save_in_ComeBet - [end]   bets:' + bets);
                    return callback(null, bets);
                }
                return callback(null, -1);
            });
        } else {
            sql = "UPDATE common SET strvalue=(strvalue::INTEGER + $1)::TEXT WHERE strkey='in_come_bets' RETURNING *";
            query(sql, [bets], function (err, res) {
                if (err) return callback(err);

                if (res.rowCount == 1) {
                    console.log('success', 'db.save_in_ComeBet - [end]   bets:' + bets);
                    return callback(null, res.rows[0].strvalue);
                }
                return callback(null, -1);
            });
        }
    });
};

/**
 * get stacked income bet form db
 * @author Bio
 * @param callback
 */
exports.getInComeBets = function (callback) {
    lib.log('success', 'db.get_income_bet - [begin]');
    var sql = "SELECT strvalue FROM common WHERE strkey='in_come_bets'";
    query(sql, function (e, r) {
        if (e) { return callback(e); }

        if (r.rowCount == 1) {
            lib.log('success', 'db.get_income_bet - [end]   in_come_bets:' + r.rows[0].strvalue);
            return callback(null, r.rows[0].strvalue);
        }

        return callback(null, 0);
    });
};

exports.checkCollectFreeDays = function (callback) {
    lib.log('success', 'db.check_collect_free_days - [begin]');
    var sql_1 = "SELECT * FROM common WHERE strkey = 'collect_free_days' OR strkey = 'welcome_bits_multiplier' OR strkey = 'welcome_free_bit'";
    var collect_free_days = 1;
    var welcome_bits_multiplier = 1;
    var welcome_free_bit = 4000;
    query(sql_1, function (err, settingList) {
        if (err) return callback(err);

        settingList = settingList.rows;
        var settingMap = {};
        for (var i = 0; i < settingList.length; i++) { settingMap[settingList[i]['strkey']] = i; }
        collect_free_days = (settingMap['collect_free_days'] != undefined) ? settingList[settingMap['collect_free_days']]['strvalue'] : 1;
        welcome_bits_multiplier = (settingMap['welcome_bits_multiplier'] != undefined) ? settingList[settingMap['welcome_bits_multiplier']]['strvalue'] : 1;
        welcome_free_bit = (settingMap['welcome_free_bit'] != undefined) ? settingList[settingMap['welcome_free_bit']]['strvalue'] : 0;

        var sql_2 = 'SELECT u.id AS user_id, welcome_free_bit, balance_satoshis ' +
                        'FROM ' +
                        '( ' +
                            'SELECT id, welcome_free_bit, balance_satoshis ' +
                            'FROM users ' +
                            'WHERE welcome_free_bit != 0 ' +
                            "AND NOT (userclass = 'admin' OR userclass = 'superadmin' OR userclass = 'staff' OR username = 'madabit' OR username = 'staff' OR username = 'ex_to_mt_' OR username = 'fun_to_mt_') " +
                            'AND (DATE(now()) - DATE(created)) >= $1 ' +
                        ') u ' +
                        'LEFT JOIN ' +
                        '( ' +
                            'SELECT * ' +
                            'FROM ' +
                            '(SELECT user_id, SUM(bet + extra_bet) sum_bet ' +
                            'FROM plays ' +
                            'GROUP BY user_id) t ' +
                            'WHERE t.sum_bet < $2 ' +
                        ') p ON u.id = p.user_id ';

        query(sql_2, [collect_free_days, welcome_free_bit * welcome_bits_multiplier], function (err, userList) {
            if (err) return callback(err);

            userList = userList.rows;
            var tasks = [];
            var total_amount = 0;
            userList.forEach(function (userInfo, index) {
                var amount = (userInfo.balance_satoshis < userInfo.welcome_free_bit) ? userInfo.balance_satoshis : userInfo.welcome_free_bit;
                var user_id = userInfo.user_id;
                tasks.push(function (callback) {
                    query('UPDATE users SET balance_satoshis = balance_satoshis - $1, welcome_free_bit = welcome_free_bit - $1 WHERE id = $2', [amount, user_id], callback);
                });
                total_amount += amount;

                console.log('db.check_collect_free_days - info - collect user - user_id:' + user_id + '   amount:' + amount);
                lib.log('info', 'db.check_collect_free_days - collect user - user_id:' + user_id + '   amount:' + amount);
            });

            if (total_amount > 0) {
                lib.log('success', 'db.check_collect_free_days - async.parallel [begin]');
                async.parallel(tasks, function (err, result) {
                    if (err) { return callback(err); }
                    lib.log('success', 'db.check_collect_free_days - async.parallel [end]');
                    lib.log('success', 'db.check_collect_free_days - update madabit balance [begin]   total_amount:' + total_amount);
                    query("UPDATE users SET balance_satoshis = balance_satoshis + $1 WHERE username = 'madabit'", [total_amount], function (err) {
                        if (err) return callback(err);
                        lib.log('success', 'db.check_collect_free_days - update madabit balance [end]   total_amount:' + total_amount);
                        lib.log('success', 'db.check_collect_free_days - [end]');
                        return callback(null, true);
                    });
                });
            } else {
                lib.log('success', 'db.check_collect_free_days - total-amount = 0 - async.parallel [skip]');
                return callback(null, true);
            }
        });
    });
};

/**
 * Get basic information
 * @author Bio
 * @param callback
 */
exports.getSyncInfo = function (callback) {
    lib.log('success', 'db.get_sync_info - [begin]');
    lib.log('success', 'db.get_sync_info - get info from db [begin]');
    var sql = 'SELECT \n' +
        "        (SELECT strvalue AS min_bet_amount FROM common WHERE strkey='min_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS max_bet_amount  FROM common WHERE strkey='max_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS min_extra_bet_amount  FROM common WHERE strkey='min_extra_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS max_extra_bet_amount  FROM common WHERE strkey='max_extra_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS extrabet_multiplier  FROM common WHERE strkey='extrabet_multiplier' LIMIT 1),\n" +
        "        (SELECT strvalue AS min_range_bet_amount  FROM common WHERE strkey='min_range_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS max_range_bet_amount  FROM common WHERE strkey='max_range_bet_amount' LIMIT 1),\n" +
        "        (SELECT strvalue AS bet_mode  FROM common WHERE strkey='bet_mode' LIMIT 1),\n" +
        "        (SELECT strvalue AS bet_mode_mobile  FROM common WHERE strkey='bet_mode_mobile' LIMIT 1),\n" +
        "        (SELECT strvalue AS show_hash  FROM common WHERE strkey='show_hash' LIMIT 1)";

    query(sql, function (e, r) {
        if (e) return callback(e);
        lib.log('success', 'db.get_sync_info - get info from db [end]');

        var result = {};
        if (r.rowCount != 1) {
            result.min_bet_amount = 1;
            result.max_bet_amount = 1000000;
            result.min_extra_bet_amount = 1;
            result.max_extra_bet_amount = 1000000;
            result.extrabet_multiplier = 50;
            result.min_range_bet_amount = 1;
            result.max_range_bet_amount = 1000000;
            result.bet_mode = 'auto_bet';
            result.bet_mode_mobile = 'custom_hide';
            result.show_hash = 'hide_hash';
            lib.log('success', 'db.get_sync_info - [end]   min_bet_amount:' + result.min_bet_amount +
                '   max_bet_amount:' + result.max_bet_amount +
                '   min_extra_bet_amount:' + result.min_extra_bet_amount +
                '   max_extra_bet_amount:' + result.max_extra_bet_amount +
                '   extrabet_multiplier:' + result.extrabet_multiplier +
                '   bet_mode:' + result.bet_mode +
                '   bet_mode_mobile:' + result.bet_mode_mobile +
                '   show_hash:' + result.show_hash
            );
            return callback(null, result);
        }

        result.min_bet_amount = r.rows[0].min_bet_amount != null ? r.rows[0].min_bet_amount : 1;
        result.max_bet_amount = r.rows[0].max_bet_amount != null ? r.rows[0].max_bet_amount : 1000000;
        result.min_extra_bet_amount = r.rows[0].min_extra_bet_amount != null ? r.rows[0].min_extra_bet_amount : 1;
        result.max_extra_bet_amount = r.rows[0].max_extra_bet_amount != null ? r.rows[0].max_extra_bet_amount : 1000000;
        result.extrabet_multiplier = r.rows[0].extrabet_multiplier != null ? r.rows[0].extrabet_multiplier : 50;
        result.min_range_bet_amount = r.rows[0].min_range_bet_amount != null ? r.rows[0].min_range_bet_amount : 1;
        result.max_range_bet_amount = r.rows[0].max_range_bet_amount != null ? r.rows[0].max_range_bet_amount : 1000000;
        result.bet_mode = r.rows[0].bet_mode != null ? r.rows[0].bet_mode : 'auto_bet';
        result.bet_mode_mobile = r.rows[0].bet_mode_mobile != null ? r.rows[0].bet_mode_mobile : 'custom_hide';
        result.show_hash = r.rows[0].show_hash != null ? r.rows[0].show_hash : 'hide_hash';

        lib.log('success', 'db.get_sync_info - [end]   min_bet_amount:' + result.min_bet_amount +
            '   max_bet_amount:' + result.max_bet_amount +
            '   min_extra_bet_amount:' + result.min_extra_bet_amount +
            '   max_extra_bet_amount:' + result.max_extra_bet_amount +
            '   extrabet_multiplier:' + result.extrabet_multiplier +
            '   bet_mode:' + result.bet_mode +
            '   bet_mode_mobile:' + result.bet_mode_mobile +
            '   show_hash:' + result.show_hash
        );
        return callback(null, result);
    });
};




/**
 * update Top5 Leaders
 * @author Bio
 * @since 2018.6.1
 * @param callback
 *
 * This method called every x minutes.
 * This method affects DB operation.
 */
exports.updateTop5Leaders = function (callback) {
    var date_str = (new Date()).toLocaleString('en-US', {timeZone: 'Asia/Shanghai'}).split(',')[0];
    var month = date_str.split('/')[0];
    var day = date_str.split('/')[1];
    var year = date_str.split('/')[2];

    if (day < 10)
        day = '0' + day;
    if (month < 10)
        month = '0' + month;

    date_str = year + '-' + month + '-' + day;

    console.log('Updating Top 5 With Date:' + date_str);

    var sql =   'SELECT t2.username, sum(t2.gross_profit) AS sum_gross_profit FROM ' +
                    '( ' +
                        'SELECT * ' +
                        'FROM ' +
                        '( ' +
                            'SELECT p.username, ' +
                            '(p.cash_out - p.bet - p.extra_bet - p.range_bet_amount) AS gross_profit ' +
                            'FROM plays p ' +
                            'WHERE p.created >= $1 and p.cash_out > 0 ' +
                        ') t ' +
                        'WHERE t.gross_profit > 0 ' +
                    ') t2 ' +
                'GROUP BY t2.username ' +
                'ORDER BY sum_gross_profit DESC ' +
                'LIMIT 5';

    query(sql, [date_str], function (err, result) {
        if (err) {
            return callback(err);
        }

        if (result.rowCount == 0)
            return callback(null);
        var tasks = [];
        var sql_delete_all = 'DELETE FROM top_players';
        var sql_insert = 'INSERT INTO top_players (id, username, profit) VALUES($1, $2, $3)';

        tasks.push(function (callback) {
            query(sql_delete_all, function (err) {
                if (err) {
                    return callback(err);
                }

                return callback(null);
            });
        });

        result.rows.forEach(function (playerInfo, index) {
            tasks.push(function(callback) {
                query(sql_insert, [index + 1, playerInfo.username, playerInfo.sum_gross_profit], function (err) {
                    if (err)
                        return callback(err);
                    return callback(err);
                });
            });
        });

        async.series(tasks, function (err) {
            if (err) {
                return callback(err);
            }
            return callback(null);
        });
    });
};
