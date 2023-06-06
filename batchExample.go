package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create table example.batches(pk int, ck int, description text, PRIMARY KEY(pk, ck));
	*/
	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	b := session.NewBatch(gocql.UnloggedBatch)
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       `INSERT INTO example.batches(pk, ck, description) VALUES(?, ?, ?)`,
		Args:       []interface{}{1, 2, "1.2"},
		Idempotent: true,
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt:       `INSERT INTO example.batches(pk, ck, description) VALUES(?, ?, ?)`,
		Args:       []interface{}{1, 3, "1,3"},
		Idempotent: true,
	})

	err = session.ExecuteBatch(b)
	if err != nil {
		panic(err)
	}

	scanner := session.Query(`SELECT pk, ck, description FROM example.batches`).Iter().Scanner()
	for scanner.Next() {
		var pk int
		var ck int
		var description string

		err = scanner.Scan(&pk, &ck, &description)
		if err != nil {
			panic(err)
		}
		fmt.Println(pk, ck, description)
	}
}
