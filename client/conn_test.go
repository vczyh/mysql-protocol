package client

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"io"
	"log"
	"math/rand"
	"testing"
)

var c *Conn

func TestMain(m *testing.M) {
	var err error
	c, err = CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"),

		WithCollation(charset.Utf8mb40900AiCi),

		WithUseSSL(true),
		WithInsecureSkipVerify(true),
		WithSSLCA("tmp/ca.pem"),
		WithSSLCert("tmp/client-cert.pem"),
		WithSSLKey("tmp/client-key.pem"),
	)
	if err != nil {
		log.Fatalf("CreateConnection(): %v", err)
	}

	setup()
	m.Run()
	shutdown()
}

func setup() {}

func shutdown() {
	if err := c.Close(); err != nil {
		log.Fatalf("Close(): %v", err)
	}
}

func TestPing(t *testing.T) {
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestExecute(t *testing.T) {
	name := fmt.Sprintf("dbtest_%d", rand.Int())
	rs, err := c.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name))
	if err != nil {
		t.Fatalf("Exec(): %v", err)
	}
	t.Log(rs.AffectedRows, rs.LastInsertId)

	rs, err = c.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name))
	if err != nil {
		t.Fatalf("Exec(): %v", err)
	}
	t.Log(rs.AffectedRows, rs.LastInsertId)
}

func TestQuery(t *testing.T) {
	//Rows, err := c.Query("SELECT @@version_comment")
	rows, err := c.Query("SHOW COLUMNS FROM mysql.user")
	if err != nil {
		t.Fatalf("Query(): %v", err)
	}

	for _, column := range rows.Columns() {
		t.Log(column.String())
	}

	for {
		row, err := rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Rows.Next(): %v", err)
		}
		t.Log(row)
	}
}
