package server

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/parser/test_driver"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/myerrors"
	"github.com/vczyh/mysql-protocol/mysql"
	"log"
)

type Handler interface {
	Command
	Listener
}

type Command interface {
	// Ping should return nil when server is healthy.
	Ping() error

	// Query performs INSERT UPDATE DELETE CREATE DROP and should return *mysql.Result.
	// Query performs SELECT and should return *ResultSet.
	Query(query string) (interface{}, error)

	// Other performs other commands.
	// data is complete command data, does not include packet header.
	Other(data []byte, conn mysql.Conn)
}

type Listener interface {
	OnConnect(connId uint32)
	OnClose(connId uint32)
}

type DefaultHandler struct{}

func NewDefaultHandler() Handler {
	return &DefaultHandler{}
}

func (*DefaultHandler) Ping() error {
	return nil
}

func (*DefaultHandler) Query(query string) (interface{}, error) {
	p := parser.New()
	stmtNode, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, myerrors.NewServer(code.ErrSendToClient, err.Error())
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
			return nil, myerrors.NewServer(code.ErrSendToClient, err.Error())
		}
		return rs, nil

	case *ast.CreateDatabaseStmt:
		return &mysql.Result{
			AffectedRows: 1,
			LastInsertId: 0,
			Status:       flag.ServerStatusAutocommit,
		}, nil

	default:
		return nil, myerrors.NewServer(code.ErrSendToClient, "unsupported statement type")
	}
}

func (*DefaultHandler) Quit() {}

func (*DefaultHandler) Other(data []byte, conn mysql.Conn) {
	if err := conn.WriteError(myerrors.NewServer(code.ErrSendToClient, "unsupported command")); err != nil {
		log.Printf("write packet error: %v\n", err)
	}
}

func (*DefaultHandler) OnConnect(connId uint32) {}

func (h *DefaultHandler) OnClose(connId uint32) {}
