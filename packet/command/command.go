package command

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

const (
	COM_SLEEP               = 0x00
	COM_QUIT                = 0x01
	COM_INIT_DB             = 0x02
	COM_QUERY               = 0x03
	COM_FIELD_LIST          = 0x04
	COM_CREATE_DB           = 0x05
	COM_DROP_DB             = 0x06
	COM_REFRESH             = 0x07
	COM_SHUTDOWN            = 0x08
	COM_STATISTICS          = 0x09
	COM_PROCESS_INFO        = 0x0a
	COM_CONNECT             = 0x0b
	COM_PROCESS_KILL        = 0x0c
	COM_DEBUG               = 0x0d
	COM_PING                = 0x0e
	COM_TIME                = 0x0f
	COM_DELAYED_INSERT      = 0x10
	COM_CHANGE_USER         = 0x11
	COM_BINLOG_DUMP         = 0x12
	COM_TABLE_DUMP          = 0x13
	COM_CONNECT_OUT         = 0x14
	COM_REGISTER_SLAVE      = 0x15
	COM_STMT_PREPARE        = 0x16
	COM_STMT_EXECUTE        = 0x17
	COM_STMT_SEND_LONG_DATA = 0x18
	COM_STMT_CLOSE          = 0x19
	COM_STMT_RESET          = 0x1a
	COM_SET_OPTION          = 0x1b
	COM_STMT_FETCH          = 0x1c
	COM_DAEMON              = 0x1d
	COM_BINLOG_DUMP_GTID    = 0x1e
	COM_RESET_CONNECTION    = 0x1f
)

// NewQuit https://dev.mysql.com/doc/internals/en/com-quit.html
func NewQuit() *generic.Simple {
	return generic.NewSimple([]byte{COM_QUIT})
}

// NewInitDB https://dev.mysql.com/doc/internals/en/com-init-db.html
func NewInitDB(db string) *generic.Simple {
	data := append([]byte{COM_INIT_DB}, db...)
	return generic.NewSimple(data)
}

func NewQuery(query string) *generic.Simple {
	data := append([]byte{COM_QUERY}, query...)
	return generic.NewSimple(data)
}

func ParseQueryResponse(data []byte) (uint64, error) {
	if len(data) < 5 {
		return 0, generic.ErrPacketData
	}
	buf := bytes.NewBuffer(data[4:])
	columnCount, err := types.LengthEncodedInteger.Get(buf)
	return columnCount, err
}

// NewCreateDB https://dev.mysql.com/doc/internals/en/com-create-db.html
func NewCreateDB(db string) *generic.Simple {
	data := append([]byte{COM_CREATE_DB}, db...)
	return generic.NewSimple(data)
}

func NewDropDB(db string) *generic.Simple {
	data := append([]byte{COM_DROP_DB}, db...)
	return generic.NewSimple(data)
}

func NewShutdown() *generic.Simple {
	return generic.NewSimple([]byte{COM_SHUTDOWN, 0x10})
}

func NewStatistics() *generic.Simple {
	return generic.NewSimple([]byte{COM_STATISTICS})
}

func NewProcessInfo() *generic.Simple {
	return generic.NewSimple([]byte{COM_PROCESS_INFO})
}

func NewProcessKill(connectionId int) *generic.Simple {
	connectionIdData := types.FixedLengthInteger.Dump(uint64(connectionId), 4)
	data := append([]byte{COM_PROCESS_KILL}, connectionIdData...)
	return generic.NewSimple(data)
}

func NewDebug() *generic.Simple {
	return generic.NewSimple([]byte{COM_DEBUG})
}

func NewPing() *generic.Simple {
	return generic.NewSimple([]byte{COM_PING})
}

func NewResetConnection() *generic.Simple {
	return generic.NewSimple([]byte{COM_RESET_CONNECTION})
}

func NewStmtPrepare(query string) *generic.Simple {
	data := append([]byte{COM_STMT_PREPARE}, query...)
	return generic.NewSimple(data)
}

func NewStmtCLost(stmtId uint32) *generic.Simple {
	stmtIdData := types.FixedLengthInteger.Dump(uint64(stmtId), 4)
	data := append([]byte{COM_STMT_CLOSE}, stmtIdData...)
	return generic.NewSimple(data)
}
