package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()
	fmt.Println("Connected Successfully!")
}
