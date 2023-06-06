package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type MyMarshaler struct {
	major int
	minor int
	patch int
}

func (m MyMarshaler) MarshalCQL(typeInfo gocql.TypeInfo) ([]byte, error) {
	return gocql.Marshal(typeInfo, fmt.Sprintf("%d.%d.%d", m.major, m.minor, m.patch))
}

func (m MyMarshaler) UnmarshalCQL(typeInfo gocql.TypeInfo, data []byte) error {
	var s string
	err := gocql.Unmarshal(typeInfo, data, &s)
	if err != nil {
		return err
	}
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return fmt.Errorf("parse version %q: %d parts instead of 3", s, len(parts))
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("parse version %q major number: %v", s, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("parse version %q minor number: %v", s, err)
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("parse version %q patch number: %v", s, err)
	}
	m.major = major
	m.minor = minor
	m.patch = patch
	return nil
}

func main() {
	/*
		create table example.my_marshaler_table(pk int, value text, PRIMARY KEY(pk));
	*/

	cluster := gocql.NewCluster("127.0.0.1", "127.0.0.2", "127.0.0.3")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	value := MyMarshaler{
		major: 1,
		minor: 2,
		patch: 3,
	}
	err = session.Query(`INSERT INTO example.my_marshaler_table(pk, value) VALUES(?, ?)`, 1, value).Exec()
	if err != nil {
		panic(err)
	}

	var stringValue string
	err = session.Query(`SELECT value FROM example.my_marshaler_table WHERE pk=1`).Scan(&stringValue)
	if err != nil {
		panic(err)
	}

	fmt.Println(stringValue)
}
