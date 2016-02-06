# I U+2764 Everyone
To celebrate Valentine's Day, I've decided to use Twitter to send a personal and heartfelt message of love to each and every one of my 300 million fellow creatures. But, as much as I love all of you, the prospect of actually typing out 300 million tweets was just not that appealing, so instead I taught my computer how to share love on my behalf.

This repo contains a simple Twitter bot script that builds up a database of sad, lonely, little users and, once every five minutes, picks a lucky lover for whom the bot tweets, "@USER, I ❤️ you". Each user will be loved at most once. At this rate, the bot should finish its first iteration in around 3000 years.

[See it in action @\_LovedArt while you still can][_lovedart].

*****

**Update**

Alas! banned after a mere 31 messages. But I've seen the error of my ways and created a [new bot to continue spreading love to everything](https://github.com/mattbierner/i-u2764-everything).

### Why?
Why not? The world needs more love.

### Isn't this spam?
No, the bot is designed to only contact users once. It also runs at a very low rate so as to not consume many of Twitter's resources. Nothing is being sold and there are [no malicious intentions](https://en.wikipedia.org/wiki/ILOVEYOU). All this bot does is post, "I ❤️ you", slowly working its way through the entire community of active Twitter users. That's it. Seriously.

I feel that this bot uses Twitter as it was intended, and improves the Twitter community by making it a more fun and random place.

### Will Twitter consider this spam?
Probably. A fun little bot like this will probably get banned after a few hours, while advertisers are free to spew all sorts of shit at users. So it goes.

Anyone is free to continue the mission of [@\_LovedArt][_lovedart] if it falls by banhammer. Love conquers all.


# Running
To spread the love yourself, first [register an application with Twitter](http://dev.twitter.com).

The main script for the bot lives in `index.js` and uses [Node][node].

```bash
$ cd i_u2764_everyone
$ npm install
```

Make sure to set the following environment variables using your fancy new registered Twitter application:

```bash
$ export TWITTER_CONSUMER_KEY="your app key"
$ export TWITTER_CONSUMER_SECRET="your app secret"
$ export TWITTER_ACCESS_TOKEN_KEY="your access token key"
$ export TWITTER_ACCESS_TOKEN_SECRET="your access token secret"
```

Then just run the script, `$ node index.js`. A database of users that tracks who has been loved so far is saved to `data.db.

Run the script using [forever][forever], `$ forever start index.js`, so the love never has to stop.


[forever]: https://github.com/foreverjs/forever
[node]: https://nodejs.org/
[_lovedart]: https://twitter.com/_lovedart
