package driver

import (
	"database/sql"
	"net/url"
	"testing"
	"time"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var err error
	db, err = sql.Open("mysql", "mysql://root:Unicloud@1221@10.0.44.59:3306")
	if err != nil {
		panic(err)
	}
	m.Run()
}

func TestURL(t *testing.T) {
	u, err := url.Parse("mysql://root:Unicloud@1221@ip:3306/db?a=b&c=d")
	if err != nil {
		t.Fatal(err)
	}

	t.Log(u.User.Username())
	if password, set := u.User.Password(); set {
		t.Logf(password)
	}

	query := u.Query()
	for k := range query {
		t.Logf("%s: %s", k, query.Get(k))
	}
}

func TestPing(t *testing.T) {
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestExecute(t *testing.T) {
	for i := 0; i < 2; i++ {
		rs, err := db.Exec("INSERT INTO db1.tb (name) VALUES (?)", "zhang")
		if err != nil {
			t.Fatal(err)
		}
		t.Log(rs.LastInsertId())
		t.Log(rs.RowsAffected())
	}
}

func TestQuery(t *testing.T) {
	for i := 0; i < 2; i++ {
		rows, err := db.Query("SELECT @@version")
		if err != nil {
			t.Fatal(err)
		}

		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("columns: %v", columns)

		var version string
		for rows.Next() {
			if err := rows.Scan(&version); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("query row: %v", version)
	}
}

func TestTime(t *testing.T) {
	_, err := db.Exec("DROP DATABASE IF EXISTS test")
	if err != nil {
		t.Fatalf("drop database failed %v", err)

	}
	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test`)
	if err != nil {
		t.Fatalf("create test db failed: %v", err)
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS test.time_tbl (
	    d DATE,
	    t TIME(6),
	    ts TIMESTAMP(6),
	    dt DATETIME(6)
	)
	`)
	if err != nil {
		t.Fatalf("create time test table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test.time_tbl VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30", "2021-01-24 13:45:30", "2021-01-24 13:45:30")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// with micro second
	_, err = db.Exec("INSERT INTO test.time_tbl VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30.123", "2021-01-24 13:45:30.123", "2021-01-24 13:45:30.123")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	_, err = db.Exec("INSERT INTO test.time_tbl VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30.123456", "2021-01-24 13:45:30.123456", "2021-01-24 13:45:30.123456")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test.time_tbl")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	var dT, tT, tsT, dtT time.Time
	for rows.Next() {
		if err := rows.Scan(&dT, &tT, &tsT, &dtT); err != nil {
			t.Fatalf("row.Scan(): %v", err)
		}
		layout := "2006-01-02 15:04:05.999999"
		t.Log(dT.Format(layout), tT.Format(layout), tsT.Format(layout), dtT.Format(layout))
	}
}
