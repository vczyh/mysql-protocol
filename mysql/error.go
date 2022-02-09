package mysql

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/generic"
)

// Error https://dev.mysql.com/doc/refman/8.0/en/error-message-elements.html
type Error interface {
	Code() generic.Code
	SQLState() string
	Message() string

	Packet() *generic.ERR

	PrintClient() string
	PrintServer() string
}

type err struct {
	code     generic.Code
	sqlState string
	message  string
}

func NewErr(code generic.Code, sqlState, message string) *err {
	return &err{
		code:     code,
		sqlState: sqlState,
		message:  message,
	}
}

func NewErrWithoutSQLState(code generic.Code, message string) *err {
	return NewErr(code, " HY000", message)
}

func (e *err) Code() generic.Code {
	return e.code
}

func (e *err) SQLState() string {
	return e.sqlState
}

func (e *err) Message() string {
	return e.message
}

func (e *err) Packet() *generic.ERR {
	return generic.NewERR(e.code, e.sqlState, e.message)
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
	ErrAccessDenied = newErrTemplate(generic.ErrAccessDeniedError, "28000", "Access denied for user '%s'@'%s' (using password: %s)")
)

type errTemplate struct {
	code     generic.Code
	sqlState string
	template string
}

func newErrTemplate(code generic.Code, sqlState, template string) *errTemplate {
	return &errTemplate{
		code:     code,
		sqlState: sqlState,
		template: template,
	}
}

func (t *errTemplate) Err(args ...interface{}) Error {
	message := fmt.Sprintf(t.template, args...)
	return NewErr(t.code, t.sqlState, message)
}
