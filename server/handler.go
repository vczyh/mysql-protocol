package server

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/parser/test_driver"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet/generic"
)

type Handler interface {
	Command
}

type Command interface {
	Ping() mysql.Error

	Query(query string) (*ResultSet, mysql.Error)

	Quit()

	Other(data []byte, conn mysql.Conn)
}

type DefaultHandler struct{}

func (h *DefaultHandler) Ping() mysql.Error {
	return nil
}

func (h *DefaultHandler) Query(query string) (*ResultSet, mysql.Error) {
	p := parser.New()
	stmtNode, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		// TODO error
		return nil, mysql.NewErr(1, "11111", err.Error())
	}

	switch stmtNode.(type) {
	case *ast.SelectStmt:
		rs, err := NewSimpleResultSet(
			[]string{"column"},
			[][]interface{}{
				{"value"},
			},
		)
		if err != nil {
			return nil, mysql.NewErrWithoutSQLState(generic.Err, err.Error())
		}
		return rs, nil

	default:
		return nil, mysql.NewErrWithoutSQLState(generic.Err, "unsupported statement type")
	}
}

func (h *DefaultHandler) Quit() {
	panic("implement me")
}

func (h *DefaultHandler) Other(data []byte, conn mysql.Conn) {
	panic("implement me")
}
