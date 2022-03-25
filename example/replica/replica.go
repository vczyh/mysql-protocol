package main

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/replica"
	"log"
)

func main() {
	r := replica.NewReplica(
		replica.WithHost("10.0.44.59"),
		replica.WithPort(3306),
		replica.WithUser("root"),
		replica.WithPassword("Unicloud@1221"),
	)

	s, err := r.StartDump("mysql-bin.000029", 4)
	if err != nil {
		log.Fatal(err)
	}

	for s.HasNext() {
		fmt.Println(s.Next())
	}

	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
}
