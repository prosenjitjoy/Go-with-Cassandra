package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
	   create table example.my_lwt_table(pk int, version int, value text, PRIMARY KEY(pk));
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = session.Query(`INSERT INTO example.my_lwt_table(pk, version, value) VALUES(?, ?, ?)`, 1, 1, "a").Exec()
	if err != nil {
		panic(err)
	}

	m := make(map[string]interface{})
	applied, err := session.Query(`UPDATE example.my_lwt_table SET value=? WHERE pk=? IF version=?`, "b", 1, 0).MapScanCAS(m)
	if err != nil {
		panic(err)
	}
	fmt.Println(applied, m)

	var value string
	err = session.Query(`SELECT value FROM example.my_lwt_table WHERE pk=?`, 1).Scan(&value)
	if err != nil {
		panic(err)
	}

	fmt.Println(value)

	m = make(map[string]interface{})
	applied, err = session.Query(`UPDATE example.my_lwt_table SET value=? WHERE pk=? IF version=?`, "b", 1, 1).MapScanCAS(m)
	if err != nil {
		panic(err)
	}
	fmt.Println(applied, m)

	var value2 string
	err = session.Query(`SELECT value FROM example.my_lwt_table WHERE pk=?`, 1).Scan(&value2)
	if err != nil {
		panic(err)
	}
	fmt.Println(value2)
}
