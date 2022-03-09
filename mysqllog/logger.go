package mysqllog

import (
	"errors"
	"github.com/vczyh/mysql-protocol/myerrors"
	"io"
	"log"
)

var (
	ErrUnknownLevel = errors.New("unknown mysql log level")
)

// Logger https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_log_error_verbosity
type Logger interface {
	System(err error)
	Info(err error)
	Warn(err error)
	Error(err error)
}

type Level uint

const (
	SystemLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (lev Level) String() string {
	switch lev {
	case SystemLevel:
		return "SYSTEM"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return ErrUnknownLevel.Error()
	}
}

type DefaultLogger struct {
	lev    Level
	out    io.Writer
	logger *log.Logger
}

func NewDefaultLogger(lev Level, out io.Writer) Logger {
	logger := log.New(out, "", log.LstdFlags)
	return &DefaultLogger{lev: lev, out: out, logger: logger}
}

func (l *DefaultLogger) System(err error) {
	l.log(SystemLevel, err)
}

func (l *DefaultLogger) Info(err error) {
	l.log(InfoLevel, err)
}

func (l *DefaultLogger) Warn(err error) {
	l.log(WarnLevel, err)
}

func (l *DefaultLogger) Error(err error) {
	l.log(ErrorLevel, err)
}

func (l *DefaultLogger) log(lev Level, err error) {
	if lev < l.lev {
		return
	}
	if myerrors.Is(err) {
		l.logger.Printf("%s %s\n", lev.String(), myerrors.Server(err))
		return
	}
	l.logger.Printf("%s %s", lev.String(), err.Error())
}
