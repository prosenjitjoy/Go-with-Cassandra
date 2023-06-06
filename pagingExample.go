package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create table example.itoa(id int, description text, PRIMARY KEY(id));
		insert into example.itoa (id, description) values (1, 'one');
		insert into example.itoa (id, description) values (2, 'two');
		insert into example.itoa (id, description) values (3, 'three');
		insert into example.itoa (id, description) values (4, 'four');
		insert into example.itoa (id, description) values (5, 'five');
		insert into example.itoa (id, description) values (6, 'six');
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	var pageState []byte
	for {
		iter := session.Query(`SELECT id, description FROM itoa`).PageSize(2).PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
		for scanner.Next() {
			var (
				id          int
				description string
			)
			err = scanner.Scan(&id, &description)
			if err != nil {
				panic(err)
			}
			fmt.Println(id, description)
		}
		err = scanner.Err()
		if err != nil {
			panic(err)
		}
		fmt.Printf("next page state: %+v\n", nextPageState)
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}
}
