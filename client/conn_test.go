package client

import "testing"

func TestCreateConnection(t *testing.T) {
	_, err := CreateConnection(
		WithHost("10.0.44.59"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("Unicloud@1221"))
	if err != nil {
		t.Fatal(err)
	}
	select {}
}
