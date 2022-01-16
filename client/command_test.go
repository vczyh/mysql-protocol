package client

import (
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := conn.Execute("select @@version")
	if err != nil {
		t.Fatal(err)
	}

	var columns []string
	for _, column := range rs.Columns {
		columns = append(columns, string(column.Name))
	}
	t.Log(columns)

	for _, row := range rs.Rows {
		t.Log(row)
	}
}

func TestQuit(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(10 * time.Second)
	if err := conn.Quit(); err != nil {
		t.Fatal(err)
	}
}

func TestInitDB(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	tables := []string{
		"test",
		"mysql",
	}
	for _, table := range tables {
		if err := conn.InitDB(table); err != nil {
			t.Error(err)
		}
	}
}
