package main

import (
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	twitter "github.com/jcla1/twitterstream"
	"io/ioutil"
	"log"
	"os"
	"time"
)

var (
	tweetsTableCreate = "CREATE TABLE IF NOT EXISTS `tweets` (" +
		"`tweet_id` bigint(20) unsigned NOT NULL," +
		"`text` varchar(140) NOT NULL DEFAULT ''," +
		"`created_at` datetime NOT NULL," +
		"`in_reply_to_status_id` bigint(20) unsigned DEFAULT NULL," +
		"`in_reply_to_user_id` int(11) unsigned DEFAULT NULL," +
		"`retweeted_status_id` bigint(20) unsigned DEFAULT NULL," +
		"`source` varchar(200) DEFAULT ''," +
		"`user_id` int(11) unsigned NOT NULL," +
		"PRIMARY KEY (`tweet_id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	usersTableCreate = "CREATE TABLE IF NOT EXISTS `users` (" +
		"`user_id` int(11) unsigned NOT NULL," +
		"`name` varchar(200) DEFAULT NULL," +
		"`screen_name` varchar(200) NOT NULL DEFAULT ''," +
		"`followers_count` int(11) NOT NULL," +
		"`friends_count` int(11) NOT NULL," +
		"`listed_count` int(11) NOT NULL," +
		"`created_at` datetime NOT NULL," +
		"`favourites_count` int(11) NOT NULL," +
		"`verified` tinyint(1) NOT NULL," +
		"`statuses_count` int(11) NOT NULL," +
		"`default_profile_image` tinyint(1) NOT NULL," +
		"PRIMARY KEY (`user_id`,`screen_name`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	userByIdSql   = "SELECT user_id FROM users WHERE user_id = ?;"
	tweetByIdSql  = "SELECT tweet_id FROM tweets WHERE tweet_id = ?;"
	putTweetSql   = "INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	putUserSql    = "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	trackKeywords = []string{"RT", "http"}
)

func main() {
	db, err := sql.Open("mysql", os.Getenv("DB_CONNECTION"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = setupTables(db)
	if err != nil {
		panic(err)
	}

	authCred, err := loadAuthCred()
	if err != nil {
		panic(err)
	}

	conn, err := setupStreamConnection(authCred)
	if err != nil {
		panic(err)
	}

	tweets := make(chan *twitter.Tweet)
	go tweetConsumer(conn, tweets)

	processTweets(db, tweets)
}

func processTweets(db *sql.DB, tweets <-chan *twitter.Tweet) {
	saveTweet := prepareTweetSaver(db)

	latestTime := time.Now()
	tweetCounter := 0

	for tweet := range tweets {
		saveTweet(tweet)
		tweetCounter += 1

		if tweetCounter%1000 == 0 {
			duration := time.Now().Sub(latestTime)
			log.Printf("current collection rate: %0.2f tweets/min", float64(tweetCounter)/(float64(duration)/float64(time.Minute)))

			latestTime = time.Now()
			tweetCounter = 0
		}
	}
}

func prepareTweetSaver(db *sql.DB) func(*twitter.Tweet) {
	getUser, getTweet := prepareGetUser(db), prepareGetTweet(db)
	putUser, putTweet := preparePutUser(db), preparePutTweet(db)

	return func(tweet *twitter.Tweet) {
	start:
		if !getUser(tweet.User) {
			putUser(tweet.User)
		}

		if !getTweet(tweet) {
			putTweet(tweet)
		}

		if tweet.RetweetedStatus != nil {
			tweet = tweet.RetweetedStatus
			goto start
		}
	}
}

func tweetConsumer(conn *twitter.Connection, c chan<- *twitter.Tweet) {
	for {
		if tweet, err := conn.Next(); err == nil {
			c <- tweet
		} else {
			close(c)
			panic(err)
		}
	}
}

func setupTables(db *sql.DB) error {
	var err error
	_, err = db.Exec(tweetsTableCreate)
	if err != nil {
		return err
	}

	_, err = db.Exec(usersTableCreate)
	if err != nil {
		return err
	}

	return nil
}

func loadAuthCred() (map[string]string, error) {
	var m map[string]string

	f, err := os.Open("config.json")
	if err != nil {
		return nil, err
	}

	contents, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(contents, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func setupStreamConnection(authCred map[string]string) (*twitter.Connection, error) {
	client := twitter.NewClient(authCred["consumer_key"], authCred["consumer_secret"], authCred["access_token_key"], authCred["access_token_secret"])
	return client.Track(trackKeywords...)
}

func preparePutUser(db *sql.DB) func(twitter.User) {
	putUserStmt, err := db.Prepare(putUserSql)
	if err != nil {
		panic(err)
	}

	return func(u twitter.User) {
		putUserStmt.Exec(u.Id, u.Name, u.ScreenName, u.FollowersCount, u.FriendsCount, u.ListedCount, u.CreatedAt.Time, u.FavouritesCount, u.Verified, u.StatusesCount, u.DefaultProfileImage)
	}
}

func prepareGetUser(db *sql.DB) func(twitter.User) bool {
	userByIdStmt, err := db.Prepare(userByIdSql)
	if err != nil {
		panic(err)
	}

	return func(user twitter.User) bool {
		row := userByIdStmt.QueryRow(user.Id)

		var id int
		err = row.Scan(&id)

		return err == nil
	}
}

func preparePutTweet(db *sql.DB) func(*twitter.Tweet) {
	putTweetStmt, err := db.Prepare(putTweetSql)
	if err != nil {
		panic(err)
	}

	return func(t *twitter.Tweet) {
		var retweetedStatusId sql.NullInt64
		if t.RetweetedStatus != nil {
			retweetedStatusId.Int64 = t.RetweetedStatus.Id
			retweetedStatusId.Valid = true
		}

		putTweetStmt.Exec(t.Id, t.Text, t.CreatedAt.Time, t.InReplyToStatusId, t.InReplyToUserId, retweetedStatusId, t.Source, t.User.Id)
	}
}

func prepareGetTweet(db *sql.DB) func(*twitter.Tweet) bool {
	tweetByIdStmt, err := db.Prepare(tweetByIdSql)
	if err != nil {
		panic(err)
	}

	return func(tweet *twitter.Tweet) bool {
		row := tweetByIdStmt.QueryRow(tweet.Id)

		var id int
		err = row.Scan(&id)

		return err == nil
	}
}
