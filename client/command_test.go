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

	querys := []string{
		"SELECT @@version",
		//"SHOW COLUMNS FROM mysql.user",
	}

	for _, query := range querys {
		rs, err := conn.Query(query)
		if err != nil {
			t.Errorf("Query: %v, Error: %v", query, err)
		}

		var columns []string
		for _, column := range rs.Columns {
			columns = append(columns, string(column.Name))
		}
		t.Log(columns)

		for _, row := range rs.Rows {
			var rowValues []string
			for _, value := range row {
				rowValues = append(rowValues, value.String())
			}
			t.Log(rowValues)
		}
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

	dbs := []string{
		"test",
		"mysql",
	}
	for _, db := range dbs {
		if err := conn.InitDB(db); err != nil {
			t.Errorf("InitDB: %v Error: %v", db, err)
		}
	}
}

func TestCreateDB(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	dbs := []string{
		"test",
		"mysql",
	}
	for _, db := range dbs {
		if err := conn.CreateDB(db); err != nil {
			t.Errorf("CreateDB: %v Error: %v", db, err)
		}
	}
}

func TestDropDB(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	dbs := []string{
		"test",
	}
	for _, db := range dbs {
		if err := conn.DropDB(db); err != nil {
			t.Errorf("DropDB: %v Error: %v", db, err)
		}
	}
}

func TestShutdown(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestStatistics(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	statistics, err := conn.Statistics()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(statistics)
}

func TestProcessInfo(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	rs, err := conn.ProcessInfo()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(rs.ColumnNames())

	for _, row := range rs.Rows {
		var rowValues []string
		for _, value := range row {
			rowValues = append(rowValues, value.String())
		}
		t.Log(rowValues)
	}
}

func TestProcessKill(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.ProcessKill(14); err != nil {
		t.Fatal(err)
	}
}

func TestDebug(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Debug(); err != nil {
		t.Fatal(err)
	}
}

func TestPing(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestResetConnection(t *testing.T) {
	conn, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.ResetConnection(); err != nil {
		t.Fatal(err)
	}
}
