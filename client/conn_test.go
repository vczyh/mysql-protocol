package client

import (
	"log"
	"testing"
)

var c Conn

func TestMain(m *testing.M) {
	var err error
	c, err = CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		log.Fatalf("CreateConnection(): %v", err)
	}

	m.Run()
	if err := c.Close(); err != nil {
		log.Fatalf("Close(): %v", err)
	}
}

func TestPing(t *testing.T) {
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
}
