package driver

import (
	"database/sql"
	"net/url"
	"testing"
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
