package myerrors

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
)

// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
var (
	AccessDenied = NewTemplate(ServerName, code.ErrAccessDeniedError, "28000", "Access denied for user '%s'@'%s' (using password: %s)")
)

type template struct {
	name     string
	code     code.Err
	sqlState string
	format   string
}

func NewTemplate(name string, c code.Err, state, format string) *template {
	return &template{
		name:     name,
		code:     c,
		sqlState: state,
		format:   format,
	}
}

func (t *template) Build(args ...interface{}) error {
	message := fmt.Sprintf(t.format, args...)
	return New(t.name, t.code, t.sqlState, message)
}
