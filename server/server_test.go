package server

import "testing"

func TestNewServer(t *testing.T) {
	srv := NewServer(
		&DefaultHandler{},
		WithHost("0.0.0.0"),
		WithPort(3307),
		WithUser("root"),
		WithPassword("root"))

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
}
