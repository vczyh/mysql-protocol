package myerrors

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
)

// https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html

const (
	ServerName  = "Server"
	SQLStateDef = "HY000"
)

type fundamental struct {
	// name represents server module, only used by server side error message.
	name     string
	code     code.Err
	sqlState string
	message  string
}

func New(name string, c code.Err, state, msg string) error {
	return &fundamental{
		name:     name,
		code:     c,
		sqlState: state,
		message:  msg,
	}
}

func NewServer(c code.Err, msg string) error {
	return New("Server", c, SQLStateDef, msg)
}

func NewServerWithSQLState(c code.Err, state, msg string) error {
	return New("Server", c, state, msg)
}

func Name(err error) string {
	if err == nil {
		return ""
	}
	if val, ok := err.(*fundamental); ok {
		return val.name
	}
	return ""
}

func Code(err error) code.Err {
	if err == nil {
		return code.ErrUndefined
	}
	if val, ok := err.(*fundamental); ok {
		return val.code
	}
	return code.ErrUndefined
}

func SQLState(err error) string {
	if err == nil {
		return SQLStateDef
	}
	if val, ok := err.(*fundamental); ok {
		return val.sqlState
	}
	return SQLStateDef
}

func Message(err error) string {
	if err == nil {
		return ""
	}
	if val, ok := err.(*fundamental); ok {
		return val.message
	}
	return ""
}

func CanSendToClient(err error) bool {
	if err == nil {
		return false
	}
	if val, ok := err.(*fundamental); ok {
		c := val.code
		return c >= 1000 && c <= 1999 || c >= 3000 && c <= 4999 || c >= 5000 && c <= 5999 || c == 50000
	}
	return false
}

func Client(err error) string {
	if err == nil {
		return ""
	}
	if val, ok := err.(*fundamental); ok {
		return fmt.Sprintf("ERROR %d (%s): %s", val.code, val.sqlState, val.message)
	}
	return ""
}

func Server(err error) string {
	if err == nil {
		return ""
	}
	if val, ok := err.(*fundamental); ok {
		return fmt.Sprintf("[MY-%06d] [%s] %s", val.code, val.name, val.message)
	}
	return ""
}

func (e *fundamental) Error() string {
	return fmt.Sprintf("%d (%s): %s", e.code, e.sqlState, e.message)
}

func Is(e error) bool {
	_, ok := e.(*fundamental)
	return ok
}
