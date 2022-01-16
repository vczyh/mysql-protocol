package test

import (
	"io"
	"net"
	"testing"
)

func TestServerGreeting(t *testing.T) {
	conn, err := net.Dial("tcp", "10.0.44.115:3306")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var buf = make([]byte, 65536)
	n, err := conn.Read(buf)
	if err != nil && err == io.EOF {
		t.Fatal(err)
	}

	t.Log(buf[:n])
}
