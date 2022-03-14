package main

import (
	"github.com/vczyh/mysql-protocol/client"
	"log"
)

func main() {
	conn, err := client.CreateConnection(
		client.WithHost("10.0.44.59"),
		client.WithPort(3306),
		client.WithUser("root"),
		//client.WithUser("native"),
		//client.WithUser("sha256_user7"),
		client.WithPassword("Unicloud@1221"))

	if err != nil {
		log.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		log.Fatalln(err)
	}
}
