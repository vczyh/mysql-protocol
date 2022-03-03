package main

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/server"
	"log"
	"sync"
)

func main() {
	userProvider := server.NewMemoryUserProvider()
	// user1
	err := userProvider.Create(&server.CreateUserRequest{
		User:        "root",
		Host:        "%",
		Password:    "123456",
		Method:      auth.SHA256Password,
		TLSRequired: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	// user2
	err = userProvider.Create(&server.CreateUserRequest{
		User:        "root2",
		Host:        "%",
		Password:    "123456",
		Method:      auth.CachingSha2Password,
		TLSRequired: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv := server.NewServer(
		userProvider,
		newTestHandler(),
		server.WithPort(3306),
		//server.WithRSAKeysDir("/Users/zhangyuheng/tmp/certs/t1"),

		//server.WithSHA256PasswordPrivateKeyPath("tmp/private_key.pem"),
		//server.WithSHA256PasswordPublicKeyPath("tmp/public_key.pem"),

		//server.WithCachingSHA2PasswordPrivateKeyPath("tmp/private_key.pem"),
		//server.WithCachingSHA2PasswordPublicKeyPath("tmp/public_key.pem"),

		//server.WithCertsDir("/Users/zhangyuheng/tmp/certs/t1"),

		//server.WithUseSSL(true),
		//server.WithSSLCA("tmp/ca.pem"),
		//server.WithSSLCert("tmp/server-cert.pem"),
		//server.WithSSLKey("tmp/server-key.pem"),
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
