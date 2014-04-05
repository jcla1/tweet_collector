until DB_CONNECTION="username:password@/dbname" caffeinate -d go run tweet_collector.go; do
	echo "\n\nThere was an error...restarting process\n\n"
	sleep 1
done
