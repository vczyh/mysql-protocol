package mysqlerror

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
)

// Error https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html
type Error interface {
	error

	Name() string

	Code() code.Code
	SQLState() string
	Message() string

	CanSendToClient() bool

	Client() string
	Server() string
}

type err struct {
	name     string
	code     code.Code
	sqlState string
	message  string
}

func New(name string, code code.Code, sqlState, message string) Error {
	e := &err{
		code:     code,
		sqlState: sqlState,
		message:  message,
	}
	if name == "" {
		e.name = "Server"
	}
	return e
}

func NewWithoutSQLState(name string, code code.Code, message string) Error {
	return New(name, code, " HY000", message)
}

func (e *err) Name() string {
	return e.name
}

func (e *err) Code() code.Code {
	return e.code
}

func (e *err) SQLState() string {
	return e.sqlState
}

func (e *err) Message() string {
	return e.message
}

func (e *err) CanSendToClient() bool {
	if e.Code() >= 1000 && e.Code() <= 1999 ||
		e.Code() >= 3000 && e.Code() <= 4999 ||
		e.Code() >= 5000 && e.Code() <= 5999 {
		return true
	}
	return false
}

func (e *err) Client() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.code, e.sqlState, e.message)
}

func (e *err) Server() string {
	return fmt.Sprintf("[MY-%06d] [%s] %s", e.code, e.name, e.message)
}

func (e *err) Error() string {
	return fmt.Sprintf("%d (%s): %s", e.code, e.sqlState, e.message)
}
