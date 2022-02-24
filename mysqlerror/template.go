package mysqlerror

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
)

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
	return t.BuildWithName("", args...)
}

func (t *template) BuildWithName(name string, args ...interface{}) Error {
	message := fmt.Sprintf(t.format, args...)
	return New(name, t.code, t.sqlState, message)
}
