-- Find the user_id's with the most tweets
SELECT COUNT(tweet_id) AS count, user_id FROM tweets
    GROUP BY user_id
    ORDER BY count DESC
    LIMIT 10;