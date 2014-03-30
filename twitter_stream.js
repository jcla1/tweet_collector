var fs = require('fs')
var twitter = require('ntwitter');
var sqlite3 = require('sqlite3').verbose();

/* VARIABLES */
var twit = new twitter(JSON.parse(fs.readFileSync('config.json')));

var db_name = 'tweet_data.sqlite';
var tweet_table_schema = 'tweet_id TEXT, text TEXT, created_at INTEGER, in_reply_to_status_id TEXT, in_reply_to_user_id INTEGER, retweeted_status_id INTEGER, source TEXT, user_id INTEGER';
var user_table_schema = 'user_id INTEGER, name TEXT, screen_name TEXT, followers_count INTEGER, friends_count INTEGER, listed_count INTEGER, created_at INTEGER, favourites_count INTEGER, verified INTEGER, statuses_count INTEGER, default_profile_image INTEGER';

var get_user_stmt;
var put_user_stmt;
var get_tweet_stmt;
var put_tweet_stmt;

var tweet_counter = 0;

function open_stream(callback) {
    log('opening tweet stream...')
    twit.stream('statuses/filter', {track: ['http', 'RT'], language: 'en'}, function(stream) {
        stream.on('data', callback);
        // stream.on('limit', handleLimit);
        stream.on('end', handleDisconnection);
        stream.on('destroy', handleDisconnection);
        stream.on('delete', handleDelete);
        stream.on('error', handleError);
    });
}

function handleDelete(response) {
    log('Delete request:');
    console.log(response);
}

function handleError(err) {
    log('Error occurred:');
    console.log(err);
}

function handleLimit(limit_msg) {
    log('INFO: You missed', limit_msg.track, 'tweets, due to Twitter\'s limiting.');
}

function handleDisconnection(response) {
    log('you were disconnected, trying to reconnect in 30s ...');

    setTimeout(function() {
        log('connecting now...');
        setup_stream();
    }, 30000);
}

function handleData(tweet) {
    tweet_counter += 1;

    // Take the tweet and check if the tweet's user is already in the db
    // If not, put him in there
    check_user(tweet.user, function() {
        // If it's a retweet, check if the original tweet is in the db
        // If not, put it in there
        // Optionally, recurse if necessary
        var retweet = tweet.retweeted_status || void(0);
        check_tweet(retweet, function() {
            // Finally put the tweet into the db
            check_tweet(tweet, function(){});
        })
    });
}

function check_user(user, callback) {
    get_user(user, function(err, row) {
        if (row != undefined) {
            callback();
            return;
        }

        put_user(user, callback)
    });
}

function check_tweet(tweet, callback) {
    if (tweet === undefined) {
        callback();
        return;
    }

    get_tweet(tweet, function(err, row) {
        if (row != undefined) {
            callback();
            return;
        }

        put_tweet(tweet, callback);
    });
}


function user_to_db_user(user) {
    return {
        $user_id: user.id_str,
        $name: user.name,
        $screen_name: user.screen_name,
        $followers_count: user.followers_count,
        $friends_count: user.friends_count,
        $listed_count: user.listed_count,
        $created_at: new Date(user.created_at).getTime() / 1000,
        $favourites_count: user.favourites_count,
        $verified: user.verified * 1,
        $statuses_count: user.statuses_count,
        $default_profile_image: user.default_profile_image * 1
    };
}

function get_user(user, callback) {
    get_user_stmt.get({
        $user_id: user.id_str
    }, callback);
}

function put_user(user, callback) {
    put_user_stmt.run(user_to_db_user(user), callback);
}

function tweet_to_db_tweet(tweet) {
    return {
        $tweet_id: tweet.id_str,
        $text: tweet.text,
        $created_at: new Date(tweet.created_at).getTime() / 1000,
        $in_reply_to_status_id: tweet.in_reply_to_status_id,
        $in_reply_to_user_id: tweet.in_reply_to_user_id,
        $retweeted_status_id: tweet.retweeted_status ? tweet.retweeted_status.id_str : null,
        $source: tweet.source,
        $user_id: tweet.user.id_str
    };
}

function get_tweet(tweet, callback) {
    get_user_stmt.get({
        $tweet_id: tweet.id_str
    }, callback);
}

function put_tweet(tweet, callback) {
    put_tweet_stmt.run(tweet_to_db_tweet(tweet), callback);
}

function setup_db(callback) {
    log('creating database...');
    var db = new sqlite3.Database(db_name);

    db.serialize(function() {
        log('creating database tables...');
        db.run('CREATE TABLE IF NOT EXISTS tweets (' + tweet_table_schema + ')');
        db.run('CREATE TABLE IF NOT EXISTS users (' + user_table_schema + ')');

        log('preparing SQL statements...')
        get_user_stmt = db.prepare('SELECT * FROM users WHERE user_id = $user_id');
        get_tweet_stmt = db.prepare('SELECT * FROM tweets WHERE tweet_id = $tweet_id');

        put_user_stmt = db.prepare('INSERT INTO users VALUES ($user_id, $name, $screen_name, $followers_count, $friends_count, $listed_count, $created_at, $favourites_count, $verified, $statuses_count, $default_profile_image)');
        put_tweet_stmt = db.prepare('INSERT INTO tweets VALUES ($tweet_id, $text, $created_at, $in_reply_to_status_id, $in_reply_to_user_id, $retweeted_status_id, $source, $user_id)');

        callback(db);
    });
}

function setup_stream(db) {
    open_stream(handleData);
}

function log() {
    if (true) console.log(Array.prototype.slice.call(arguments, 0).join(' '));
}

function main() {
    setup_db(setup_stream);
    setTimeout(function() {
        log('INFO: Up to', new Date(), 'you recieved', tweet_counter, 'tweets');
        tweet_counter = 0;
        setTimeout(arguments.callee, 10000);
    }, 10000)
}

main()