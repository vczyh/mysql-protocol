package main

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/server"
	"log"
	"sync"
)

func main() {
	userProvider := server.NewMemoryUserProvider()
	err := userProvider.Create(&server.CreateUserRequest{
		User:     "root",
		Host:     "%",
		Password: "Unicloud@1221",
		//Method:      core.CachingSha2Password,
		Method: core.SHA256Password,
		//Method:      core.MySQLNativePassword,
		TLSRequired: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv := server.NewServer(
		userProvider,
		newTestHandler(),
		server.WithPort(3306),
		//server.WithDefaultAuthMethod(core.CachingSha2Password),
		//server.WithDefaultAuthMethod(core.SHA256Password),

		server.WithCachingSHA2PasswordPrivateKeyPath("tmp/private_key.pem"),
		server.WithCachingSHA2PasswordPublicKeyPath("tmp/public_key.pem"),
	)

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
