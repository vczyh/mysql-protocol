package errors

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/packet"
)

// MySQLError https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html
type MySQLError interface {
	error

	Code() core.Code
	SQLState() string
	Message() string

	Packet() *packet.ERR

	PrintClient() string
	PrintServer() string
}

type err struct {
	code     core.Code
	sqlState string
	message  string
}

func New(code core.Code, sqlState, message string) *err {
	return &err{
		code:     code,
		sqlState: sqlState,
		message:  message,
	}
}

func NewWithoutSQLState(code core.Code, message string) *err {
	return New(code, " HY000", message)
}

func (e *err) Code() core.Code {
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

func (e *err) PrintClient() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.code, e.sqlState, e.message)
}

func (e *err) PrintServer() string {
	return fmt.Sprintf("[MY-%06d] [Server] %s", e.code, e.message)
}

func (e *err) Error() string {
	return fmt.Sprintf("%d (%s): %s", e.code, e.sqlState, e.message)
}

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
var (
	AccessDenied = newErrTemplate(core.ErrAccessDeniedError, "28000", "Access denied for user '%s'@'%s' (using password: %s)")
)

type errTemplate struct {
	code     core.Code
	sqlState string
	template string
}

func newErrTemplate(code core.Code, sqlState, template string) *errTemplate {
	return &errTemplate{
		code:     code,
		sqlState: sqlState,
		template: template,
	}
}

func (t *errTemplate) Err(args ...interface{}) MySQLError {
	message := fmt.Sprintf(t.template, args...)
	return New(t.code, t.sqlState, message)
}
