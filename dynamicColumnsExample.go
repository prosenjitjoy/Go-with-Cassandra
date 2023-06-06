package main

import (
	"fmt"
	"os"
	"reflect"
	"text/tabwriter"

	"github.com/gocql/gocql"
)

func main() {
	/*
	   create table example.table1(pk text, ck int, value1 text, value2 int, PRIMARY KEY(pk, ck));
	   insert into example.table1 (pk, ck, value1, value2) values ('a', 1, 'b', 2);
	   insert into example.table1 (pk, ck, value1, value2) values ('c', 3, 'd', 4);
	   insert into example.table1 (pk, ck, value1, value2) values ('c', 5, null, null);
	   create table example.table2(pk int, value1 timestamp, PRIMARY KEY(pk));
	   insert into example.table2 (pk, value1) values (1, '2020-01-02 03:04:05');
	*/
	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	err = printQuery(session, "SELECT * FROM table1")
	if err != nil {
		panic(err)
	}

	err = printQuery(session, "SELECT value2, pk, ck FROM table1")
	if err != nil {
		panic(err)
	}

	err = printQuery(session, "SELECT * FROM table2")
	if err != nil {
		panic(err)
	}
}

func printQuery(session *gocql.Session, stmt string, values ...interface{}) error {
	iter := session.Query(stmt, values...).Iter()
	fmt.Println(stmt)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	for i, column := range iter.Columns() {
		if i > 0 {
			fmt.Fprint(w, "\t| ")
		}
		fmt.Fprintf(w, "%s (%s)", column.Name, column.TypeInfo)
	}

	for {
		rd, err := iter.RowData()
		if err != nil {
			return err
		}
		if !iter.Scan(rd.Values...) {
			break
		}
		fmt.Fprint(w, "\n")
		for i, val := range rd.Values {
			if i > 0 {
				fmt.Fprint(w, "\t| ")
			}
			fmt.Fprint(w, reflect.Indirect(reflect.ValueOf(val)).Interface())
		}
	}

	fmt.Fprint(w, "\n")
	w.Flush()
	fmt.Println()

	return iter.Close()
}
