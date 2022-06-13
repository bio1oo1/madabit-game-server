var assert = require('better-assert');
var async = require('async');
var db = require('./database');
var events = require('events');
var util = require('util');
var _ = require('lodash');
var lib = require('./lib');
var SortedArray = require('./sorted_array');
var config = require('./config');

var tickRate = 150; // ping the client every X miliseconds
var afterCrashTime = 3000; // how long from game_crash -> game_starting
var restartTime = 8000; // How long from  game_starting -> game_started


// create new game
function Game (lastGameId, lastHash, bankroll, gameHistory) {
    var self = this;

    // initialize all member variables
    self.bankroll = bankroll;
    self.fakepool = 0;
    self.agent_sys_fee_pro = 0;

    self.maxWin = 0;

    self.gameShuttingDown = false;
    self.startTime = null; // time game started. If before game started, is an estimate...
    self.crashPoint = 0; // when the game crashes, 0 means instant crash
    self.gameDuration = 0; // how long till the game will crash..

    self.forcePoint = null; // The point we force terminate the game

    self.state = 'ENDED'; // 'STARTING' | 'BLOCKING' | 'IN_PROGRESS' |  'ENDED'
    self.pending = {}; // Set of players pending a joined
    self.pendingCount = 0;
    self.joined = new SortedArray(); // A list of joins, before the game is in progress

    self.players = {}; // An object of userName ->  { playId: ..., autoCashOut: .... }
    self.gameId = lastGameId;
    self.gameHistory = gameHistory;

    self.lastHash = lastHash;
    self.hash = null;

    self.forceFinishGame = false;
    self.setNextCrash0 = false;

    self.bets = [];
    self.extraBets = [];
    self.rangeBets = [];

    events.EventEmitter.call(self);

    function runGame () {
        lib.log('info', 'game.run_game - [begin]');

        if (config.GAME_CLOSE == true) {
            var beijing_time_array = (new Date()).toLocaleString('en-US', {timeZone: 'Asia/Shanghai'}).split(' ');
            if (beijing_time_array[2] == 'AM') {
                var beijing_time_hour = parseInt(beijing_time_array[1].split(':')[0]);
                if (beijing_time_hour >= 2 && beijing_time_hour < 6) {
                    // var tmp = parseInt(Math.random() * 10) % 2;
                    // var flag = (tmp == 0) ? true : false;
                    self.emit('setMaintenance', {
                        maintenance: true
                    });
                    setTimeout(runGame, 30000);
                    return;
                }
            }

            self.emit('setMaintenance', {
                maintenance: false
            });
        } else {
            self.emit('setMaintenance', {
                maintenance: false
            });
        }

        db.createGame(self.gameId + 1, function (err, info) {
            lib.log('error', 'game.run_game - after db.create_game');
            if (err) {
                lib.log('error', 'game.run_game - could not create game 1 ' + err + ' retrying in 2 sec...');
                console.log('Could not create game 1 ', err, ' retrying in 2 sec..');
                setTimeout(runGame, 2000);
                return;
            }

            db.getMaxProfit(function (err, max_profit) {
                lib.log('info', 'game.run_game - after db.get_max_profit');
                if (err) {
                    lib.log('error', 'game.run_game - could not create game 2 ' + err + ' retrying in 2 sec...');
                    console.log('Could not create game 2 ', err, ' retrying in 2 sec..');
                    setTimeout(runGame, 2000);
                    return;
                }

                self.state = 'STARTING';
                self.crashPoint = (info.crashPoint);

                if (config.CRASH_AT) {
                    lib.log('error', 'game.run_game - could not create game 2 ' + err + 'retrying in 2 sec...');
                    self.crashPoint = Math.round(config.CRASH_AT);
                }

                // self.crashPoint = 600;

                if (self.setNextCrash0) {
                    self.crashPoint = 0;
                    self.setNextCrash0 = false;
                }

                self.hash = info.hash;
                self.gameId++;
                self.startTime = new Date(Date.now() + restartTime);
                self.players = {}; // An object of userName ->  { user: ..., playId: ..., autoCashOut: ...., status: ... }
                self.gameDuration = Math.ceil(inverseGrowth(self.crashPoint + 1)); // how long till the game will crash..
                self.maxWin = Math.round(self.bankroll * parseFloat(max_profit) / 100); // Risk 3% per game

                self.emit('game_starting', {
                    game_id: self.gameId,
                    max_win: self.maxWin,
                    time_till_start: restartTime
                });

                setTimeout(blockGame, restartTime);

                // send all basic bet information to web server
                db.getSyncInfo(function (err, sync_info) {
                    if (err) console.log(err);
                    self.emit('update_bet_info', {
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

                    db.getRangeInfo(undefined, function (err, range_info) {
                        if (err) console.log(err);
                        self.emit('update_range_info', range_info);

                        db.checkCollectFreeDays(function (err, bCollect) {
                            if (err) console.log('checkCollectFreeDays : ', err);
                            // if (bCollect == false)
                            // {
                            //     console.log("no need collect free days");
                            // }
                        });
                    });
                });
            });
        });
    }

    function blockGame () {
        self.state = 'BLOCKING'; // we're waiting for pending bets..

        lib.log('info', 'game.block_game - [begin]');
        loop();
        function loop () {
            if (self.pendingCount > 0) {
                lib.log('info', 'game.block_game - delaying game by 100ms for ' + self.pendingCount + ' joins');
                console.log('Delaying game by 100ms for ', self.pendingCount, ' joins');
                return setTimeout(loop, 100);
            }
            lib.log('info', 'game.block_game - [end]');
            startGame();
        }
    }

    function startGame () {
        lib.log('info', 'game.start_game - [begin]');
        self.state = 'IN_PROGRESS';
        self.startTime = new Date();
        self.pending = {};
        self.pendingCount = 0;

        var bets = {};
        var extraBets = {};
        var rangeBets = {};
        var demos = {};
        var arr = self.joined.getArray();

        var nTotalBets = 0;
        for (var i = 0; i < arr.length; ++i) {
            var a = arr[i];
            bets[a.user.username] = a.bet;
            extraBets[a.user.username] = a.extraBet;
            rangeBets[a.user.username] = a.rangeBetInfo;
            demos[a.user.username] = a.user.demo;
            self.players[a.user.username] = a;
            self.players[a.user.username].demo = a.user.demo;

            if (a.user.demo === false) {
                nTotalBets += (a.bet + a.extraBet + a.rangeBetInfo.amount);
            }
        }

        self.bets = bets;
        self.extraBets = extraBets;
        self.rangeBets = rangeBets;

        self.joined.clear();

        // self.emit('refreshAllPlayerProfit', {});

        // get all agent system fee percent
        db.getAgentSysFeePro(function (err, agent_sys_fee_pro) {
            var nInComeBet = Math.round(nTotalBets * agent_sys_fee_pro / 100);
            db.saveInComeBets(nInComeBet, function (err, nInComeBets) {
                self.emit('game_started',
                    {
                        bets: bets,
                        extraBets: extraBets,
                        rangeBets: rangeBets,
                        demos: demos,
                        in_come_bets: nInComeBets / 100
                    });
            });
        });

        db.getSyncInfo(function (err, sync_info) {
            if (err) console.log(err);
            lib.log('info', 'game.start_game - emit update_bet_info   min_bet_amount:' + sync_info.min_bet_amount +
                    '   max_bet_amount:' + sync_info.max_bet_amount +
                    '   min_extra_bet_amount:' + sync_info.min_extra_bet_amount +
                    '   max_extra_bet_amount:' + sync_info.max_extra_bet_amount +
                    '   extrabet_multiplier:' + sync_info.extrabet_multiplier +
                    '   bet_mode:' + sync_info.bet_mode +
                    '   bet_mode_mobile:' + sync_info.bet_mode_mobile +
                    '   show_hash :' + sync_info.show_hash);

            self.emit('update_bet_info', {
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

            db.getRangeInfo(undefined, function (err, range_info) {
                if (err) console.log(err);
                self.emit('update_range_info', range_info);

                lib.log('info', 'game.run_cashout - call set_force_point');
                self.setForcePoint();
                callTick(0);
            });
        });
    }

    function callTick (elapsed) {
        var left = self.gameDuration - elapsed;
        var nextTick = Math.max(0, Math.min(left, tickRate));

        setTimeout(runTick, nextTick);
    }

    function runTick () {
        var elapsed = new Date() - self.startTime;
        var at = growthFunc(elapsed);

        self.runCashOuts(at);

        if (self.forcePoint <= at && self.forcePoint <= self.crashPoint) {
            console.log('game forced out - game_id:' + self.gameId + '   force_point:' + self.forcePoint + '   at:' + at + '   crash_point:' + self.crashPoint);
            lib.log('info', 'game forced out - game_id:' + self.gameId + '   force_point:' + self.forcePoint + '   at:' + at + '   crash_point:' + self.crashPoint);

            //* *********** give max_profit to remaining players when forced out.
            // self.cashOutAll(self.forcePoint, function (err) {
            //     console.log('Just forced cashed out everyone at: ', self.forcePoint, ' got err: ', err);
            //     console.log('cashOutAll : bankroll - ' + self.bankroll + ', gameId - ' + self.gameId + ', crashPoint - ' + self.crashPoint);
            //     endGame(true);
            // });

            //* ********** treat forced_out as normal busted.
            self.crashPoint = self.forcePoint;
            endGame(true);
            return;
        }

        if (self.forceFinishGame === true) { // admin can set this flag
            self.forceFinishGame = false;
            console.log('administrator stoped game - bankroll:' + self.bankroll + '   game_id:' + self.gameId + '   crash_point:' + self.crashPoint);
            lib.log('info', 'administrator stoped game - bankroll:' + self.bankroll + '   game_id:' + self.gameId + '   crash_point:' + self.crashPoint);
            endGame(true);
            return;
        }

        // and run the next

        if (at > self.crashPoint) {
            endGame(false); // oh noes, we crashed!
        } else { tick(elapsed); }
    }

    function endGame (forced) {
        var gameId = self.gameId;
        var crashTime = Date.now();

        lib.log('info', 'game.endgame -    game_id:' + gameId + '   forced:' + forced + '   crash_point:' + self.crashPoint);
        console.log('info', 'game.endgame -    game_id:' + gameId + '   forced:' + forced + '   crash_point:' + self.crashPoint);

        assert(self.crashPoint == 0 || self.crashPoint >= 100); // Bio
        self.lastHash = self.hash;
        self.emit('game_crash',
            {
                forced: forced,
                elapsed: self.gameDuration,
                game_crash: self.crashPoint, // We send 0 to client in instant crash
                hash: self.lastHash
            });

        db.getSyncInfo(function (err, sync_info) {
            if (err) console.log(err);
            self.emit('update_bet_info',
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

        db.getRangeInfo(undefined, function (err, range_info) {
            if (err) console.log(err);
            self.emit('update_range_info', range_info);
        });

        // get real bankroll info from db and update
        db.getBankroll(function (err, bankroll) {
            if (err) console.log('Error : getBankroll');
            self.bankroll = parseInt(bankroll);
            db.getFakePool(function (err, fakepool) {
                if (err) console.log('Error : getFakepool');
                self.fakepool = fakepool;
                db.getAgentSysFeePro(function (err, agent_sys_fee_pro) {
                    if (err) console.log('Error : getAgenSysFeePro');
                    self.agent_sys_fee_pro = agent_sys_fee_pro;

                    self.emit('update_bankroll',
                        {
                            bankroll: self.bankroll,
                            fakepool: self.fakepool,
                            agent_sys_fee_pro: agent_sys_fee_pro
                        });
                });
            });
        });

        var playerInfo = self.getInfo().player_info;

        self.gameHistory.addCompletedGame({
            game_id: gameId,
            game_crash: self.crashPoint,
            created: self.startTime,
            player_info: playerInfo,
            hash: self.lastHash
        });

        var dbTimer;
        dbTimeout();
        function dbTimeout () {
            dbTimer = setTimeout(function () {
                console.log('warning', 'game - ' + gameId + ' is still ending... Time since crash: ' + ((Date.now() - crashTime) / 1000).toFixed(3) + 's');
                lib.log('warning', 'game - ' + gameId + ' is still ending... Time since crash: ' + ((Date.now() - crashTime) / 1000).toFixed(3) + 's');

                dbTimeout();
            }, 1000);
        }

        db.getExtraBetMultiplier(function (err, extrabet_multiplier) {
            if (err) {
                console.log('error', 'game - getExtraBetMultiplier');
                lib.log('error', 'game - getExtraBetMultiplier');
            }
            db.endGame(gameId, self.crashPoint, extrabet_multiplier, function (err, totalUserProfitMap) {
                if (isNaN(gameId)) console.log('end game : gameId is nan');
                if (isNaN(self.crashPoint)) console.log('end game : self.creashPoint is nan');

                // if (play.extraBet > 0 && self.crashPoint == 0) ->extraBetSuccess
                if (err) { console.log('ERROR could not end game id: ', gameId, ' got err: ', err); }

                clearTimeout(dbTimer);

                self.emit('add_satoshis', totalUserProfitMap);

                if (self.gameShuttingDown) {
                    self.emit('shutdown');
                } else {
                    setTimeout(runGame, (crashTime + afterCrashTime) - Date.now());
                }
            });
        });

        self.state = 'ENDED';
    }

    function tick (elapsed) {
        self.emit('game_tick', elapsed);
        callTick(elapsed);
    }

    runGame();
}

util.inherits(Game, events.EventEmitter);

Game.prototype.getInfo = function () {
    var playerInfo = {};

    for (var username in this.players) {
        var record = this.players[username];

        assert(lib.isInt(record.bet));
        var info = {
            bet: parseInt(record.bet),
            extraBet: parseInt(record.extraBet),
            rangeBet: record.rangeBetInfo,
            demo: record.user.demo
        };

        if (record.status === 'CASHED_OUT') {
            assert(lib.isInt(record.stoppedAt));
            info['stopped_at'] = record.stoppedAt;
        }

        playerInfo[username] = info;
    }

    var res = {
        state: this.state,
        player_info: playerInfo,
        game_id: this.gameId, // game_id of current game, if game hasnt' started its the last game
        last_hash: this.lastHash,
        max_win: this.maxWin,
        // if the game is pending, elapsed is how long till it starts
        // if the game is running, elapsed is how long its running for
        /// if the game is ended, elapsed is how long since the game started
        elapsed: Date.now() - this.startTime,
        created: this.startTime,
        joined: this.joined.getArray().map(function (u) { return u.user.username; })
    };

    if (this.state === 'ENDED') { res.crashed_at = this.crashPoint; }

    return res;
};

// Calls callback with (err, booleanIfAbleToJoin)
Game.prototype.placeBet = function (user, betAmount, extraBet, rangeBetInfo, autoCashOut, callback) {
    lib.log('info', 'game.place_bet - [begin]   user:' + user.username + '   bet_amount:' + betAmount + ' ' +
        'userna  extra_bet:' + extraBet + '   rangeBet:' + rangeBetInfo.amount + ', ' + rangeBetInfo.id + '   auto_cashout:' + autoCashOut);
    var self = this;

    assert(typeof user.id === 'number');
    assert(typeof user.username === 'string');
    assert(lib.isInt(betAmount));
    if (Object.keys(rangeBetInfo).length == 0) { assert(lib.isInt(autoCashOut) && autoCashOut >= 100); } else assert(lib.isInt(autoCashOut) && autoCashOut == 0);

    if (self.state !== 'STARTING') {
        return callback('GAME_IN_PROGRESS');
    }

    if (lib.hasOwnProperty(self.pending, user.username) || lib.hasOwnProperty(self.players, user.username)) {
        lib.log('info', 'game.place_bet - already place bet   user:' + user.username + '   bet_amount:' + betAmount +
                '   extra_bet:' + extraBet + '   range_bet:' + JSON.stringify(rangeBetInfo) + '   auto_cashout:' + autoCashOut);
        return callback('ALREADY_PLACED_BET');
    }

    self.pending[user.username] = user.username;
    self.pendingCount++;

    console.log('game.placeBet : ', 'bet : ', betAmount, '   extraBet:', extraBet, '   rangeBet:', rangeBetInfo, 'autoCasOut : ', autoCashOut, 'username : ', user.id, 'gameId : ', self.gameId);

    var checkLoginBonusCount = 0;

    db.getRanges(function (err, ranges) {
        var tasks = [];
        var range_bet_result_array = [];
        if (Object.keys(rangeBetInfo).length == 0) {
            var range_bet_result = [];
            range_bet_result[0] = {};
            range_bet_result[0].range_from = -1;
            range_bet_result[0].range_to = -1;
            range_bet_result[0].range_multiplier = 0;
            range_bet_result[0].amount = 0;
            range_bet_result[0].range_id = -1;
            tasks.push(function (callback) {
                db.placeBet(betAmount, extraBet, range_bet_result[0], autoCashOut, user.id, self.gameId, function (err, playId) {
                    if (err) {
                        if (err.code == '23514') { // constraint violation
                            lib.log('error', 'game.place_bet - not enough money   user:' + user.username + '   bet_amount:' +
                                    betAmount + '   extra_bet:' + extraBet + '   auto_cashout:' + autoCashOut);
                            console.log('db.placeBet : err : not enough money');
                            return callback('NOT_ENOUGH_MONEY');
                        }
                        lib.log('error', 'game.place_bet - internal error could not play game, got error ' + err + '   user:' + user.username +
                                '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '   auto_cashout:' + autoCashOut);
                        console.log('[INTERNAL_ERROR] could not play game, got error: ', err);
                        callback(err);
                    } else {
                        assert(playId > 0);

                        var index = self.joined.insert({
                            user: user,
                            bet: betAmount,
                            extraBet: extraBet,
                            rangeBetInfo: range_bet_result,
                            autoCashOut: autoCashOut,
                            playId: playId,
                            status: 'PLAYING'
                        });
                        lib.log('info', 'game.placebet - insert to self.joined    index:' + index + '   user:' + user.username + '   play_id:' + playId);
                        console.log('info', 'game.placebet - insert to self.joined    index:' + index + '   user:' + user.username + '   play_id:' + playId);

                        self.emit('player_bet', {
                            username: user.username,
                            index: index,
                            demo: user.demo
                        });

                        // Check all free bonuses
                        var tasksAfterBet = [];

                        tasksAfterBet.push(function (callback) {
                            db.checkCanBeAgent(user.id, function (err) {
                                if (err) return callback(err);
                                return callback(null);
                            });
                        });

                        tasksAfterBet.push(function (callback) {
                            db.checkFirstDepositFee(user.id, playId, function (err, fd_info) {
                                if (err) return callback(err);
                                if (fd_info == undefined) { return callback(null); }
                                if (fd_info.msg == 'GAVE_FEE') {
                                    db.notifyFundingBonus(fd_info.toAccount, user.username, fd_info.fAvailableFee, function (err) {
                                        if (err) return callback(err);
                                        db.getReplyCheck(user.id, function (err, replylist) {
                                            if (err) return callback(err);
                                            self.emit('got_first_deposit_fee', {
                                                username: fd_info.toAccount,
                                                replylist: replylist,
                                                clientname: user.username,
                                                fAvailableFee: fd_info.fAvailableFee
                                            });
                                            return callback(null);
                                        });
                                    });
                                } else {
                                    return callback(null);
                                }
                            });
                        });
                        if (user.demo === false) {
                            console.log('info', 'game.place_bet - login_bonus   user_id:', user.id, '   play_id:', playId);
                            tasksAfterBet.push(function (callback) {
                                db.checkLoginBonus(user.id, playId, user.time_zone, function (err, lgResult) {
                                    if (err) return callback(err);
                                    if (lgResult == undefined) { return callback(null); }
                                    if (lgResult.msg == 'GAVE_BONUS') {
                                        db.notifyLoginBonus(user.id, lgResult.bonus / 100, function (err) {
                                            if (err) return callback(err);
                                            self.emit('got_login_bonus',
                                                {
                                                    username: user.username,
                                                    got_login_bonus: lgResult.bonus
                                                });
                                            return callback(null);
                                        });
                                    } else {
                                        return callback(null);
                                    }
                                });
                            });
                        }

                        async.series(tasksAfterBet, function (err) {
                            if (err) return callback(err);
                            return callback(null);
                        });
                    }
                });
            });
        } else {
            var rangesMap = {};

            for (var i = 0; i < ranges.length; i++) {
                var range = ranges[i];
                var id = range.id;
                rangesMap[id] = {};
                rangesMap[id].range_from = range.range_from;
                rangesMap[id].range_to = range.range_to;
                rangesMap[id].range_multiplier = range.range_multiplier;
                rangesMap[id].range_id = range.id;
            }
            Object.keys(rangeBetInfo).forEach(function (range_id) {
                tasks.push(function (callback) {
                    var range_bet_result = rangesMap[range_id];
                    if (range_bet_result == undefined) { return callback(null); }

                    console.log('game.placebet -  selected_range_id:' + range_id + '   range_bet_result:' + range_bet_result);

                    range_bet_result.amount = rangeBetInfo[range_id];
                    range_bet_result_array.push(range_bet_result);
                    db.placeBet(betAmount, extraBet, JSON.parse(JSON.stringify(range_bet_result)), autoCashOut, user.id, self.gameId, function (err, playId) {
                        if (err) {
                            if (err.code == '23514') { // constraint violation
                                lib.log('error', 'game.place_bet - not enough money   user:' + user.username + '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '   auto_cashout:' + autoCashOut);
                                console.log('db.placeBet : err : not enough money');
                                return callback('NOT_ENOUGH_MONEY');
                            }

                            lib.log('error', 'game.place_bet - internal error could not play game, got error ' + err + '   user:' + user.username + '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '   auto_cashout:' + autoCashOut);
                            console.log('[INTERNAL_ERROR] could not play game, got error: ', err);
                            return callback(err);
                        }

                        assert(playId > 0);
                        var index = self.joined.insert({
                            user: user,
                            bet: betAmount,
                            extraBet: extraBet,
                            rangeBetInfo: range_bet_result_array,
                            autoCashOut: autoCashOut,
                            playId: playId,
                            status: 'PLAYING'
                        });
                        lib.log('info', 'game.placebet - insert to self.joined    index:' + index + '   user:' + user.username + '   play_id:' + playId);
                        console.log('info', 'game.placebet - insert to self.joined    index:' + index + '   user:' + user.username + '   play_id:' + playId);

                        self.emit('player_bet', {
                            username: user.username,
                            index: index,
                            demo: user.demo
                        });

                        // Check all free bonuses
                        if (user.demo == false) {
                            lib.log('info', 'game.place_bet - demo:false   user:' + user.username + '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '   auto_cashout:' + autoCashOut);

                            var tasksAfterBet = [];

                            tasksAfterBet.push(function (callback) {
                                db.checkCanBeAgent(user.id, function (err) {
                                    if (err) return callback(err);
                                    return callback(null);
                                });
                            });

                            tasksAfterBet.push(function (callback) {
                                db.checkFirstDepositFee(user.id, playId, function (err, fd_info) {
                                    if (err) return callback(err);
                                    if (fd_info != undefined && fd_info.msg == 'GAVE_FEE') {
                                        // db.notifyFundingBonus(fd_info.toAccount, user.username, fd_info.fAvailableFee, function (err) {
                                        //     if (err) return callback(err);
                                        db.getReplyCheck(user.id, function (err, replylist) {
                                            if (err) return callback(err);
                                            self.emit('got_first_deposit_fee', {
                                                username: fd_info.toAccount,
                                                replylist: replylist,
                                                clientname: user.username,
                                                fAvailableFee: fd_info.fAvailableFee
                                            });
                                            return callback(null);
                                        });
                                        // });
                                    } else {
                                        return callback(null);
                                    }
                                });
                            });
                            if (user.demo === false && checkLoginBonusCount == 0) {
                                console.log('info', 'game.place_bet - login_bonus   user_id:', user.id, '   play_id:', playId);
                                checkLoginBonusCount++;
                                tasksAfterBet.push(function (callback) {
                                    db.checkLoginBonus(user.id, playId, user.time_zone, function (err, lgResult) {
                                        if (err) return callback(err);
                                        if (lgResult == undefined) { return callback(null); }
                                        if (lgResult.msg == 'GAVE_BONUS') {
                                            db.notifyLoginBonus(user.id, lgResult.bonus / 100, function (err) {
                                                if (err) return callback(err);
                                                self.emit('got_login_bonus',
                                                    {
                                                        username: user.username,
                                                        got_login_bonus: lgResult.bonus
                                                    });
                                                return callback(null);
                                            });
                                        } else {
                                            return callback(null);
                                        }
                                    });
                                });
                            }

                            async.series(tasksAfterBet, function (err) {
                                if (err) return callback(err);
                                return callback(null);
                            });
                        } else {
                            return callback(null);
                        }
                    });
                });
            });
        }

        async.series(tasks, function (err, result) {
            db.getBankroll(function (err, bankroll) {
                if (err) {
                    lib.log('error', 'game.place_bet - get_bank_roll   user:' + user.username +
                        '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '    range_bet:' + rangeBetInfo.amount +
                        ', ' + rangeBetInfo.id + '   auto_cashout:' + autoCashOut);
                    console.log('Error : getBankroll');
                    return callback(err);
                }
                self.bankroll = parseInt(bankroll);
                lib.log('info', 'game.place_bet - bankroll:' + bankroll);
                db.getFakePool(function (err, fakepool) {
                    if (err) {
                        lib.log('error', 'game.place_bet - get_fake_pool   user:' + user.username +
                                '   bet_amount:' + betAmount + '   extra_bet:' + extraBet + '   range_bet:' + rangeBetInfo.amount +
                                ', ' + rangeBetInfo.id + '   auto_cashout:' + autoCashOut);
                        console.log('Error : getFakepool');
                        return callback(err);
                    }
                    self.fakepool = fakepool;
                    lib.log('info', 'game.place_bet - fakepool:' + fakepool);
                    db.getAgentSysFeePro(function (err, agent_sys_fee_pro) {
                        if (err) {
                            lib.log('error', 'game.place_bet - get_agent_sys_fee_pro');
                            console.log('Error : getAgentSysFeePro');
                            return callback(err);
                        }
                        self.agent_sys_fee_pro = agent_sys_fee_pro;
                        lib.log('info', 'game.place_bet - agent_sys_fee_pro:' + agent_sys_fee_pro);
                        self.emit('update_bankroll', {
                            bankroll: self.bankroll,
                            fakepool: self.fakepool,
                            agent_sys_fee_pro: self.agent_sys_fee_pro
                        });
                        self.pendingCount--;
                        return callback(null);
                    });
                });
            });
        });
    });
};

Game.prototype.doCashOut = function (play, at, extraSuccess, callback) {
    assert(typeof play.user.username === 'string');
    assert(typeof play.user.id === 'number');
    assert(typeof play.playId === 'number');
    assert(typeof at === 'number');
    assert(typeof callback === 'function');

    lib.log('info', 'game.do_cashout - [begin]  user:' + play.user.username + '   play_id:' + play.playId + '   at:' + at + '   extra_success:' + extraSuccess);

    var self = this;

    var username = play.user.username;

    assert(self.players[username].status === 'PLAYING');
    self.players[username].status = 'CASHED_OUT';
    self.players[username].stoppedAt = at;

    db.getExtraBetMultiplier(function (err, extrabet_multiplier) {
        extrabet_multiplier = parseInt(extrabet_multiplier);

        var won = (self.players[username].bet / 100) * at;
        var extraBet = self.players[username].extraBet * (extrabet_multiplier + 1);
        assert(lib.isInt(won));

        if (extraSuccess === true) {
            won = parseInt(self.players[username].bet);
        } else {
            extraBet = 0;
        }

        db.cashOut(play.user.id, play.playId, won, extraBet, extraSuccess, play.user.demo, extrabet_multiplier, function (err) {
            if (err) {
                lib.log('info', 'game.do_cashout - [begin]  play:' + play + '   at:' + at + '   extra_success:' + extraSuccess + '[INTERNAL_ERROR] could not cash out: ' + ' at ' + at + ' in ' + play + 'becuse:' + err);
                console.log('[INTERNAL_ERROR] could not cash out: ', username, ' at ', at, ' in ', play, ' because: ', err);
                return callback(err);
            }

            // self.emit('add_satoshis', totalUserProfitMap);
            self.emit('cashed_out',
                {
                    username: username,
                    stopped_at: at,
                    extraSuccess: extraSuccess,
                    add_bits: won + extraBet
                });
            lib.log('info', 'game.do_cashout - [end]  user:' + play.user.username + '   play_id:' + play.playId + '   at:' + at + '   extra_success:' + extraSuccess);
            return callback(null);
        });
    });
};

Game.prototype.runCashOuts = function (at) {
    var self = this;

    var update = false;
    // Check for auto cashouts

    Object.keys(self.players).forEach(function (playerUserName) {
        var play = self.players[playerUserName];

        if (play.status === 'CASHED_OUT') {
            console.log('game.run_cashout - return_cashed_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
            lib.log('info', 'game.run_cashout - return_cashed_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
            return;
        }

        assert(play.status === 'PLAYING');
        if (play.bet !== 0) { // not range bet
            assert(play.autoCashOut);
            if (play.extraBet > 0 && self.crashPoint === 0) {
                self.doCashOut(play, 0, true, function (err) {
                    if (err) {
                        console.log('[INTERNAL_ERROR] could not auto cashout ', playerUserName, ' at ', play.autoCashOut);
                        lib.log('info', 'game.run_cashout - [INTERNAL_ERROR] could not auto cashout ' + playerUserName + ' at ' + play.autoCashOut);
                    } else {
                        console.log('game.run_cashout - cash_out_auto - success - do_cash_out_extrabet - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
                        lib.log('success', 'game.run_cashout - cash_out_auto - do_cash_out_extrabet - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
                    }
                });
            } else if (play.autoCashOut <= at && play.autoCashOut <= self.crashPoint && play.autoCashOut <= self.forcePoint) {
                self.doCashOut(play, play.autoCashOut, false, function (err) {
                    if (err) {
                        console.log('[INTERNAL_ERROR] could not auto cashout ', playerUserName, ' at ', play.autoCashOut);
                        lib.log('info', 'game.run_cashout - [INTERNAL_ERROR] could not auto cashout ' + playerUserName + ' at ' + play.autoCashOut);
                    } else {
                        console.log('game.run_cashout - cash_out_auto - success - do_cash_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
                        lib.log('success', 'game.run_cashout - cash_out_auto - do_cash_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
                    }
                });
            } else {
                console.log('game.run_cashout - cash_out_auto - overflow - do_cash_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
                lib.log('success', 'game.run_cashout - cash_out_auto - overflow - do_cash_out - user:' + playerUserName + '   auto_cash_out:' + play.autoCashOut + '   at:' + at + '   crash_point:' + self.crashPoint);
            }

            update = true;
        }
    });

    if (update) {
        lib.log('info', 'game.run_cashout - call set_force_point');
        self.setForcePoint();
    }
};

/**
 * Fninsh a game manually by admin
 */
Game.prototype.setForcePoint = function () {
    lib.log('info', 'game.set_force_point - [begin]');
    /// /////////////// no forced out ////////////////////////////////////////////////////////
    var self = this;
    // self.forcePoint = Infinity; // the game can go until it crashes, there's no end.
    /// /////////////// no forced out ////////////////////////////////////////////////////////

    /// /////////////// set forced out mode //////////////////////////////////////////////////
    var totalBet = 0; // how much satoshis is still in action
    var totalCashedOut = 0; // how much satoshis has been lost

    Object.keys(self.players).forEach(function (playerName) {
        var play = self.players[playerName];

        if (play.status === 'CASHED_OUT') {
            totalCashedOut += (play.bet * (play.stoppedAt - 100) / 100);
        } else {
            assert(play.status === 'PLAYING');
            assert(lib.isInt(play.bet));
            totalBet += play.bet;
        }
    });

    lib.log('info', 'game.set_force_point    totalBet:' + totalBet);

    if (totalBet === 0) {
        self.forcePoint = Infinity; // the game can go until it crashes, there's no end.
        if (self.forcePoint !== self.prev_fp) {
            lib.log('info', 'game.set_force_point    totalBet:' + totalBet + '   game_id:' + self.gameId + '   forced_point:Infinity');
            console.log('set forced_point - total_bet:' + totalBet + '   game_id:' + self.gameId + '   forced_point:Infinity');
        }
    } else {
        var left = self.maxWin - totalCashedOut - (totalBet * 0.01);
        var ratio = (left + totalBet) / totalBet;

        // in percent
        self.forcePoint = Math.max(Math.floor(ratio * 100), 101);
        if (self.forcePoint !== self.prev_fp) {
            lib.log('info', 'game.set_force_point    totalBet:' + totalBet + '   game_id:' + self.gameId + '   forced_point:' + self.forcePoint);
            console.log('set forced_point - total_bet:' + totalBet + '   game_id:' + self.gameId + '   forced_point:' + self.forcePoint);
        }
    }

    // if (self.forcePoint != Infinity && self.forcePoint < self.crashPoint) {
    //     self.crashPoint = Math.round(self.forcePoint);
    // }// fake crash point // 20180424

    self.prev_fp = self.forcePoint;
    /// /////////////// set forced out mode //////////////////////////////////////////////////
};

Game.prototype.cashOut = function (user, callback) {
    lib.log('info', 'game.cashout - [begin]   user:' + user.username);
    var self = this;

    assert(typeof user.id === 'number');

    if (this.state !== 'IN_PROGRESS') {
        lib.log('error', 'game.cashout -  game not in progress  user:' + user.username);
        return callback('GAME_NOT_IN_PROGRESS');
    }

    var elapsed = new Date() - self.startTime;
    var at = growthFunc(elapsed);
    var play = lib.getOwnProperty(self.players, user.username);

    if (!play) {
        lib.log('error', 'game.cashout - no_bet_placed  user:' + user.username);
        return callback('NO_BET_PLACED');
    }

    console.log('game.cashout - user:' + user.username + '   elapsed:' + elapsed + '   at:' + at + '   autoCashOut:' + play.autoCashOut + '   forcePoint:' + self.forcePoint);
    lib.log('info', 'game.cashout - user:' + user.username + '   elapsed:' + elapsed + '   at:' + at + '   autoCashOut:' + play.autoCashOut + '   forcePoint:' + self.forcePoint);

    if (play.autoCashOut <= at) { at = play.autoCashOut; }

    if (self.forcePoint <= at) { at = self.forcePoint; }

    if (at > self.crashPoint) {
        lib.log('error', 'game.cashout - game  user:' + user.username);
        return callback('GAME_ALREADY_CRASHED');
    }

    if (play.status === 'CASHED_OUT') {
        lib.log('error', 'game.cashout - already cashed out  user:' + user.username);
        return callback('ALREADY_CASHED_OUT');
    }

    self.doCashOut(play, at, false, callback);
    self.setForcePoint();
    lib.log('info', 'game.cashout - [end]   user:' + user.username);
};

Game.prototype.cashOutAll = function (at, callback) {
    lib.log('info', 'game.cashoutall - [begin]   at:' + at);
    var self = this;

    if (this.state !== 'IN_PROGRESS') { return callback(); }

    lib.log('info', 'game.cashoutall - cashing everyone out at:' + at);
    console.log('Cashing everyone out at: ', at);

    assert(at >= 100);

    self.runCashOuts(at);

    if (at > self.crashPoint) { return callback(); } // game already crashed, sorry guys

    var tasks = [];

    Object.keys(self.players).forEach(function (playerName) {
        var play = self.players[playerName];

        if (play.status === 'PLAYING') {
            tasks.push(function (callback) {
                if (play.status === 'PLAYING') {
                    self.doCashOut(play, at, false, callback);
                } else {
                    callback();
                }
            });
        }
    });

    console.log('Needing to force cash out: ', tasks.length, ' players');
    lib.log('info', 'game.cashoutall - cashing everyone out at:' + at);

    async.parallelLimit(tasks, 4, function (err) {
        if (err) {
            console.error('[INTERNAL_ERROR] unable to cash out all players in ', self.gameId, ' at ', at);
            lib.error('error', 'game.cashoutall - cashing everyone out at:' + at);
            callback(err);
            return;
        }

        lib.error('error', 'game.cashoutall - emergency cashed out all players in game_id:' + self.gameId);
        console.log('emergency cashed out all players in game_id: ', self.gameId);
        callback();
    });
};

Game.prototype.shutDown = function () {
    var self = this;

    self.gameShuttingDown = true;
    lib.log('info', 'game.shutdown - emit shuttingdown');
    self.emit('shuttingdown');

    // If the game has already ended, we can shutdown immediately.
    if (this.state === 'ENDED') {
        lib.log('info', 'game.shutdown - emit shuttingdown');
        self.emit('shutdown');
    }
};

Game.prototype.finishRound = function (currentTime, currentPoint, gameId) {
    if (currentPoint == null || gameId != this.gameId) {
        this.crashPoint = 1000;
        console.log('===========================================================');
        return;
    }
    this.forceFinishGame = true;
    this.crashPoint = Math.round(currentPoint * 100);
    this.gameDuration = currentTime;
    lib.log('info', 'game.finishround -    force_finish_game:' + this.forceFinishGame);
    lib.log('info', 'game.finishround -    force_finish_game:' + this.crashPoint);
    lib.log('info', 'game.finishround -    force_finish_game:' + this.gameDuration);
};

/**
 * Set crash point of next game to 0
 * @author Bio
 */
Game.prototype.setNext0 = function () {
    lib.log('info', 'game.finishround -    set_next_round as 0:');
    this.setNextCrash0 = true;
};

function growthFunc (ms) {
    var r = 0.00006;
    return Math.floor(100 * Math.pow(Math.E, r * ms));
}

function inverseGrowth (result) {
    var c = 16666.666667;
    return c * Math.log(0.01 * result);
}

module.exports = Game;
