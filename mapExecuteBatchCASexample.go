package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	/*
	   create table example.my_lwt_batch_table(pk text, ck text, version int, value text, PRIMARY KEY(pk, ck));
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = session.Query(`INSERT INTO example.my_lwt_batch_table(pk, ck, version, value) VALUES(?, ?, ?, ?)`, "pk1", "ck1", 1, "a").Exec()
	if err != nil {
		panic(err)
	}

	err = session.Query(`INSERT INTO example.my_lwt_batch_table(pk, ck, version, value) VALUES(?, ?, ?, ?)`, "pk1", "ck2", 1, "A").Exec()
	if err != nil {
		panic(err)
	}

	executeBatch(session, 0)
	printState(session)
	executeBatch(session, 1)
	printState(session)
}

func executeBatch(session *gocql.Session, ckv int) {
	b := session.NewBatch(gocql.LoggedBatch)
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: "UPDATE my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		Args: []interface{}{"b", "pk1", "ck1", 1},
	})
	b.Entries = append(b.Entries, gocql.BatchEntry{
		Stmt: "UPDATE my_lwt_batch_table SET value=? WHERE pk=? AND ck=? IF version=?",
		Args: []interface{}{"B", "pk1", "ck2", ckv},
	})
	m := make(map[string]interface{})
	applied, iter, err := session.MapExecuteBatchCAS(b, m)
	if err != nil {
		panic(err)
	}
	fmt.Println(applied, m)

	m = make(map[string]interface{})
	for iter.MapScan(m) {
		fmt.Println(m)
		m = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		panic(err)
	}
}

func printState(session *gocql.Session) {
	scanner := session.Query(`SELECT ck, value FROM example.my_lwt_batch_table WHERE pk=?`, "pk1").Iter().Scanner()
	for scanner.Next() {
		var ck, value string
		err := scanner.Scan(&ck, &value)
		if err != nil {
			panic(err)
		}
		fmt.Println(ck, value)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
