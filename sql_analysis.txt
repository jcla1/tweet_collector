-- Find the number of tweets in the DB
SELECT COUNT(*) AS count FROM tweets;

-- Find the number of users in the DB
SELECT COUNT(*) AS count FROM users;

-- Find the user_id's with the most tweets
SELECT COUNT(tweet_id) AS count, user_id FROM tweets
    GROUP BY user_id
    ORDER BY count DESC
    LIMIT 10;

-- Find the tweet_id's with the most retweets
SELECT retweeted_status_id, COUNT(*) AS count FROM tweets
    WHERE retweeted_status_id IS NOT NULL
    GROUP BY retweeted_status_id
    ORDER BY count DESC
    LIMIT 10;

-- Averge number of retweets in the DB
SELECT AVG(count) AS average FROM
    (
        SELECT COUNT(*) AS count FROM tweets
        WHERE retweeted_status_id IS NOT NULL
        GROUP BY retweeted_status_id
        ORDER BY count DESC
    ) AS counts;
