package server

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/parser/test_driver"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/mysqlerror"
	"log"
)

type Handler interface {
	Command
	Listener
}

type Command interface {
	Ping() error
	Query(query string) (ResultSet, error)
	Quit()
	Other(data []byte, conn mysql.Conn)
}

type Listener interface {
	OnConnect(connId uint32)
	OnClose(connId uint32)
}

type DefaultHandler struct{}

func NewDefaultHandler() *DefaultHandler {
	return &DefaultHandler{}
}

func (h *DefaultHandler) Ping() error {
	return nil
}

func (h *DefaultHandler) Query(query string) (ResultSet, error) {
	p := parser.New()
	stmtNode, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, mysqlerror.NewWithoutSQLState("", code.ErrGeneral, err.Error())
	}

	switch v := stmtNode.(type) {
	case *ast.SelectStmt:
		rs, err := NewSimpleResultSet(
			[]string{v.Fields.Fields[0].Text()},
			[][]interface{}{
				{"Value"},
			},
		)
		if err != nil {
			return nil, mysqlerror.NewWithoutSQLState("", code.ErrGeneral, err.Error())
		}
		return rs, nil

	default:
		return nil, mysqlerror.NewWithoutSQLState("", code.ErrGeneral, "unsupported statement type")
	}
}

func (h *DefaultHandler) Quit() {
	// empty implement
}

func (h *DefaultHandler) Other(data []byte, conn mysql.Conn) {
	if err := conn.WriteError(mysqlerror.NewWithoutSQLState("", code.ErrGeneral, "unsupported command")); err != nil {
		log.Printf("write packet error: %v\n", err)
	}
}

func (h *DefaultHandler) OnConnect(connId uint32) {
	// empty implement
}

func (h *DefaultHandler) OnClose(connId uint32) {
	// empty implement
}
