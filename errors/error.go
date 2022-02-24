package errors

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/packet"
)

// Error https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html
type Error interface {
	error

	Code() code.Code
	SQLState() string
	Message() string

	Packet() *packet.ERR

	Client() string
	Server() string
}

type err struct {
	code     code.Code
	sqlState string
	message  string
}

func New(code code.Code, sqlState, message string) Error {
	return &err{
		code:     code,
		sqlState: sqlState,
		message:  message,
	}
}

func NewWithoutSQLState(code code.Code, message string) Error {
	return New(code, " HY000", message)
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

func (e *err) Packet() *packet.ERR {
	return packet.NewERR(e.code, e.sqlState, e.message)
}

func (e *err) Client() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.code, e.sqlState, e.message)
}

func (e *err) Server() string {
	return fmt.Sprintf("[MY-%06d] [Server] %s", e.code, e.message)
}

func (e *err) Error() string {
	return fmt.Sprintf("%d (%s): %s", e.code, e.sqlState, e.message)
}

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
var (
	AccessDenied = newTemplate(code.ErrAccessDeniedError, "28000", "Access denied for user '%s'@'%s' (using password: %s)")
)

type template struct {
	code     code.Code
	sqlState string
	format   string
}

func newTemplate(code code.Code, sqlState, format string) *template {
	return &template{
		code:     code,
		sqlState: sqlState,
		format:   format,
	}
}

func (t *template) Build(args ...interface{}) Error {
	message := fmt.Sprintf(t.format, args...)
	return New(t.code, t.sqlState, message)
}
