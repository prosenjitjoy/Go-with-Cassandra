package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
		create type example.my_udt (field_a text, field_b int);
		create table example.my_udt_table(pk int, value frozen<my_udt>, PRIMARY KEY(pk));
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	value := map[string]interface{}{
		"field_a": "a value",
		"field_b": 42,
	}

	err = session.Query(`INSERT INTO example.my_udt_table(pk, value) VALUES(?, ?)`, 1, value).Exec()
	if err != nil {
		panic(err)
	}

	var readValue map[string]interface{}
	err = session.Query(`SELECT value FROM example.my_udt_table WHERE pk=1`).Scan(&readValue)
	if err != nil {
		panic(err)
	}
	fmt.Println(readValue["field_a"])
	fmt.Println(readValue["field_b"])
}
