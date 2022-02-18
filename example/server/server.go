package main

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/server"
	"log"
	"sync"
)

func main() {
	srv := server.NewServer(
		newTestHandler(),
		server.WithHost("0.0.0.0"),
		server.WithPort(3306),
		server.WithUser("root"),
		server.WithPassword("root"))

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}

type testHandler struct {
	dh        *server.DefaultHandler
	connIdSet sync.Map
}

func newTestHandler() *testHandler {
	h := new(testHandler)
	h.dh = server.NewDefaultHandler()
	return h
}

func (h *testHandler) Ping() error {
	return h.dh.Ping()
}

func (h *testHandler) Query(query string) (server.ResultSet, error) {
	return h.dh.Query(query)
}

func (h *testHandler) Quit() {
	fmt.Println("QUIT command called")
}

func (h *testHandler) Other(data []byte, conn mysql.Conn) {
	h.dh.Other(data, conn)
}

func (h *testHandler) OnConnect(connId uint32) {
	fmt.Println("new connect: ", connId)
	h.connIdSet.Store(connId, "")
}

func (h *testHandler) OnClose(connId uint32) {
	fmt.Println("close connect: ", connId)
	h.connIdSet.Delete(connId)
}
