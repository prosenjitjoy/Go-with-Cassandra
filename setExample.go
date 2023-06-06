package main

import (
	"fmt"
	"sort"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create table example.sets(id int, value set<text>, PRIMARY KEY(id));
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = session.Query(`INSERT INTO sets(id, value) VALUES(?, ?) IF NOT EXISTS`, 1, []string{"alpha", "be", "gamma"}).Exec()
	if err != nil {
		panic(err)
	}

	err = session.Query(`UPDATE sets SET value=? WHERE id=1`, []string{"alpha", "beta", "gamma"}).Exec()
	if err != nil {
		panic(err)
	}

	err = session.Query(`UPDATE sets SET value=value+? WHERE id=1`, "epsilon").Exec()
	if err != nil {
		fmt.Printf("expected error: %v\n", err)
	}

	err = session.Query(`UPDATE sets SET value=value+? WHERE id=1`, []string{"delta"}).Exec()
	if err != nil {
		panic(err)
	}

	toRemove := map[string]struct{}{
		"alpha": {},
		"gamma": {},
	}

	err = session.Query(`UPDATE sets SET value=value-? WHERE id=1`, toRemove).Exec()
	if err != nil {
		panic(err)
	}

	scanner := session.Query(`SELECT id, value FROM sets`).Iter().Scanner()
	for scanner.Next() {
		var (
			id  int
			val []string
		)
		err := scanner.Scan(&id, &val)
		if err != nil {
			panic(err)
		}
		sort.Strings(val)
		fmt.Printf("Row %d is %v\n", id, val)
	}

	err = scanner.Err()
	if err != nil {
		panic(err)
	}
}
