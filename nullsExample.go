package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create table example.stringvals(id int, value text, PRIMARY KEY(id));
		insert into example.stringvals (id, value) values (1, null);
		insert into example.stringvals (id, value) values (2, '');
		insert into example.stringvals (id, value) values (3, 'hello');
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	scanner := session.Query(`SELECT id, value FROM stringvals`).Iter().Scanner()
	for scanner.Next() {
		var (
			id  int
			val *string
		)
		err := scanner.Scan(&id, &val)
		if err != nil {
			panic(err)
		}
		if val != nil {
			fmt.Printf("Row %d is %q\n", id, *val)
		} else {
			fmt.Printf("Row %d is null\n", id)
		}
	}

	err = scanner.Err()
	if err != nil {
		panic(err)
	}
}
