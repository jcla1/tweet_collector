package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	twitter "github.com/darkhelmet/twitterstream"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
)

var _ = fmt.Println

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
	for tweet := range tweets {

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

func prepareGetUser(db *sql.DB) func(*twitter.User) bool {
	userByIdStmt, err := db.Prepare(userByIdSql)
	if err != nil {
		panic(err)
	}

	return func(user *twitter.User) bool {
		row := userByIdStmt.QueryRow(user.Id)
		if err != nil {
			panic(err)
		}

		var id int
		err = row.Scan(&id)

		return err == nil
	}
}

func prepareGetTweet(db *sql.DB) func(*twitter.Tweet) bool {
	tweetByIdStmt, err := db.Prepare(tweetByIdSql)
	if err != nil {
		panic(err)
	}

	return func(tweet *twitter.Tweet) bool {
		row := tweetByIdStmt.QueryRow(tweet.Id)
		if err != nil {
			panic(err)
		}

		var id int
		err = row.Scan(&id)

		return err == nil
	}
}
