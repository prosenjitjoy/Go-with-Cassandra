package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
		create index on example.tweet(timeline);
	*/
	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = session.Query(`INSERT INTO tweet(timeline, id, text) VALUES(?, ?, ?)`, "me", gocql.TimeUUID(), "Hello World!").Exec()
	if err != nil {
		panic(err)
	}

	err = session.Query(`INSERT INTO tweet(timeline, id, text) VALUES(?, ?, ?)`, "you", gocql.TimeUUID(), "Welcome").Exec()
	if err != nil {
		panic(err)
	}

	var id gocql.UUID
	var text string

	err = session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`, "me").Consistency(gocql.One).Scan(&id, &text)
	if err != nil {
		panic(err)
	}
	fmt.Println("Tweet:", id, text)
	fmt.Println()

	scanner := session.Query(`SELECT id, text FROM tweet`).Iter().Scanner()

	for scanner.Next() {
		err = scanner.Scan(&id, &text)
		if err != nil {
			panic(err)
		}
		fmt.Println("Tweet:", id, text)
	}

	if err = scanner.Err(); err != nil {
		panic(err)
	}
}
