Tweet collector
===============

Use it to collect tweets from the [Twitter streaming API](https://dev.twitter.com/docs/api/1.1/post/statuses/filter).

## Usage
First you need API credentials for the streaming API. You can get those by creating a new Twitter App on the Application management screen. Put these credentials in a JSON file called `config.json`.
Then edit keepalive.sh and put in your DB credentials (tested with MySQL) in the env variable `DB_CREDENTIALS`.
Example: username:password@/dbname
If you're not on OSX remove the call to "caffeinate".
The script will run the collector, that saves tweets and users to the DB, and in case of an error, it will restart the process. Forever.
