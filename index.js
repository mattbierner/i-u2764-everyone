"use strict";
const path = require('path');
const fs = require('fs');
const Twitter = require('twitter');
const Datastore = require('nedb');

// Time between postings, in ms.
const INTERVAL = 1000 * 60 * 5; // five minutes.

// Database file location.
const DB_FILE = "data.db";
const DB_PATH = path.join(__dirname, DB_FILE);

// Minimum number of unprocessed users to keep around at any given time.
const QUEUE_SIZE = 100;

// Maximum number of users to sample each time. May not be unique.
const BUFFER_SIZE = 50;

// Disable actual postings
const DEBUG = true;

// Keep around in-memory cache to prevent getting stuck on a single item.
const CACHE_LIMIT = 10;

let temp_cache = [];

const add_cache = (x) => {
    temp_cache.unshift(x);
    // unduplicate
    temp_cache = temp_cache.filter((x, pos) => temp_cache.indexOf(x) === pos);
    while (temp_cache.length > CACHE_LIMIT)
        temp_cache.pop();
};


const client = new Twitter({
    consumer_key: process.env.TWITTER_CONSUMER_KEY,
    consumer_secret: process.env.TWITTER_CONSUMER_SECRET,
    access_token_key: process.env.TWITTER_ACCESS_TOKEN_KEY,
    access_token_secret: process.env.TWITTER_ACCESS_TOKEN_SECRET,
});

const db = new Datastore({ filename: DB_FILE, autoload: true });

/**
    Select a user from the database,
*/
const get_user_entry = (user, k) =>
    db.findOne({ user: user }, k);

/**
    Try to add a user to the database if one does not already exist.
*/
const ensure_user_entry = (user) =>
    new Promise((resolve, reject) =>
        db.update({ user: user }, { $set: { user: user } }, { upsert: true },
            (err, result) =>
                err ? reject(err) : resolve(result)));

/**
    Get users who are in database but have not been messaged yet.
*/
const get_unloved_user_entries = () =>
    new Promise((resolve, reject) =>
        db.find({ messaged_at: { $exists: false } },
            (err, result) => err ? reject(err) : resolve(result)));

/**
    Start filling in the user database using the sample stream.
*/
const populate_users = (k) =>
    client.stream('statuses/sample', {}, (stream) => {
        stream.on('data', tweet => {
            if (tweet && tweet.user && tweet.user.screen_name) {
                if (!k(tweet.user.screen_name))
                    stream.destroy();
            }
        });

        stream.on('error', e => {
            console.error('stream error', e);
        });
    });

/**
    Try to make sure we have a good set of unloved users to pick from.
*/
const ensure_future_users = () =>
    get_unloved_user_entries()
        .then(users => {
            if (users.length > QUEUE_SIZE)
                return;

            return new Promise((resolve) => {
                let required = BUFFER_SIZE;
                populate_users(user => {
                    ensure_user_entry(user).catch(e => console.error('ensure error', e));
                    if (required-- < 0) {
                        resolve();
                        return false; /* done */
                    }
                    return true; /* continue */
                });
            });
        });

/**
    Select a user who needs some love.
*/
const pick_user = () =>
    new Promise((resolve, reject) =>
        db.findOne({ $and: [ { messaged_at: { $exists: false } }, { user: { $nin: temp_cache } }] },
            (err, result) => {
                if (err)
                    return reject(err);
                if (!result)
                    return reject("no one to share the love with :(");
                resolve(result.user);
            }));

/**
    Make sure not to share too much love by marking when a user has been messaged.
*/
const update_user = (user) =>
    new Promise((resolve, reject) =>
        db.update({ user: user },
            { $set: { messaged_at: Date.now() }},
            {},
            (err) => err ? reject(err) : resolve(user)));

/**
    Generate an expression of boundless love.
*/
const status_message = (user) =>
    '@' + user + ", I \u{2764} you";

/**
    Post the love.
*/
const share_the_love = (user) => {
    add_cache(user);

    return new Promise((resolve, reject) =>
        DEBUG ? resolve(user) :
        client.post('statuses/update', { status: status_message(user) },
            (err, tweet, response) =>
                err ? reject(err) : resolve(user)));
};

/**
    Spread love to the world.
*/
const spread_the_love = () =>
    ensure_future_users().then(pick_user).then(user =>
        share_the_love(user)
            .then(update_user)
            .then(user => {
                console.log('Shared the love with ' + user);
                setTimeout(spread_the_love, INTERVAL);
            })
            .catch(err => {
                console.error('post error', err);
                if (err && err[0] && err[0].code === 187) {
                    console.error('Duplicate detected by Twitter, added entry to cache and will retry', user);
                    setTimeout(spread_the_love, 250);
                } else {
                    setTimeout(spread_the_love, INTERVAL);
                }
            }))
    .catch(err => {
        console.error('pick error', err);
    });

db.ensureIndex({ fieldName: 'user' }, err => {
    if (err) {
        console.error('index error', err);
        return;
    }
    spread_the_love();
});
