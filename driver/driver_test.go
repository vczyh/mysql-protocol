package driver

import (
	"database/sql"
	"log"
	"math"
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

	setup()
	m.Run()
	shutdown()
}

func setup() {
	_, err := db.Exec("DROP DATABASE IF EXISTS test")
	if err != nil {
		log.Fatalf("drop database failed %v", err)

	}
	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS test`)
	if err != nil {
		log.Fatalf("create test db failed: %v", err)
	}
}

func shutdown() {
	if err := db.Close(); err != nil {
		log.Fatalf("close db failed: %v", err)
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

func TestBigint(t *testing.T) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS test.tbl_bigint (
	    col_bigint bigint,
	    col_bigint_unsigned bigint unsigned
	)
	`)
	if err != nil {
		t.Fatalf("create tbl_int failed: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test.tbl_bigint VALUES (?,?)")
	if err != nil {
		t.Fatalf("db.Prepare(): %v", err)
	}

	// normal
	_, err = stmt.Exec(-101, 101)
	if err != nil {
		t.Fatalf("insert(tbl_bigint) max: %v", err)
	}

	// max
	_, err = stmt.Exec(math.MaxInt64, uint64(math.MaxUint64))
	if err != nil {
		t.Fatalf("insert(tbl_bigint) max: %v", err)
	}

	// min
	_, err = stmt.Exec(math.MinInt64, 0)
	if err != nil {
		t.Fatalf("insert(tbl_bigint) max: %v", err)
	}

	if err := stmt.Close(); err != nil {
		t.Fatalf("stmt.Close(): %v", err)
	}

	rows, err := db.Query("SELECT * FROM test.tbl_bigint")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	for rows.Next() {
		var bigint int64
		var unBigint uint64
		if err := rows.Scan(&bigint, &unBigint); err != nil {
			t.Fatalf("rows.scan(): %v", err)
		}
		t.Log(bigint, unBigint)
	}
}

func TestTime(t *testing.T) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS test.tbl_time (
	    d DATE,
	    t TIME(6),
	    ts TIMESTAMP(6),
	    dt DATETIME(6)
	)
	`)
	if err != nil {
		t.Fatalf("create time test table failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test.tbl_time VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30", "2021-01-24 13:45:30", "2021-01-24 13:45:30")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// with micro second
	_, err = db.Exec("INSERT INTO test.tbl_time VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30.123", "2021-01-24 13:45:30.123", "2021-01-24 13:45:30.123")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test.tbl_time VALUES (?,?,?,?)", "2021-01-24", "-5 13:45:30.123456", "2021-01-24 13:45:30.123456", "2021-01-24 13:45:30.123456")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	_, err = db.Exec("INSERT INTO test.tbl_time VALUES (?,?,?,?)", "2021-01-24", "-34 13:45:30.123456", "2021-01-24 13:45:30.123456", "2021-01-24 13:45:30.123456")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test.tbl_time")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	var dT, tsT, dtT time.Time
	var tT int
	for rows.Next() {
		if err := rows.Scan(&dT, &tT, &tsT, &dtT); err != nil {
			t.Fatalf("row.Scan(): %v", err)
		}
		layout := "2006-01-02 15:04:05.999999"
		t.Log(dT.Format(layout), time.Duration(tT), tsT.Format(layout), dtT.Format(layout))
	}
}

func TestDecimal(t *testing.T) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS test.tbl_decimal (
	    d1 DECIMAL,
	    d2 DECIMAL(20,10)
	)
	`)
	if err != nil {
		t.Fatalf("create tbl_decimal failed: %v", err)
	}

	stmt, err := db.Prepare("INSERT INTO test.tbl_decimal VALUES (?,?)")
	if err != nil {
		t.Fatalf("db.Prepare(): %v", err)
	}

	_, err = stmt.Exec("10.000000001", "10.000000001")
	if err != nil {
		t.Fatalf("insert into test.tbl_decimal: %v", err)
	}

	rows, err := db.Query("SELECT * FROM test.tbl_decimal")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	for rows.Next() {
		var d1, d2 string
		if err := rows.Scan(&d1, &d2); err != nil {
			t.Fatalf("rows.scan(): %v", err)
		}
		t.Log(d1, d2)
	}
}
