package packet

import (
	"bytes"
)

type Command byte

// https://dev.mysql.com/doc/internals/en/command-phase.html
const (
	ComSleep Command = iota
	ComQuit
	ComInitDB
	ComQuery
	ComFieldList
	ComCreateDB
	ComDropDB
	ComRefresh
	ComShutdown
	ComStatistics
	ComProcessInfo
	ComConnect
	ComProcessKill
	ComDebug
	ComPing
	ComTime
	ComDelayedInsert
	ComChangeUser
	ComBinlogDump
	ComTableDump
	ComConnectOut
	ComRegisterSlave
	ComStmtPrepare
	ComStmtExecute
	ComStmtSendLongData
	ComStmtClose
	ComStmtReset
	ComSetOption
	ComStmtFetch
	ComDaemon
	ComBinlogDumpGTID
	ComResetConnection
)

func (c Command) Byte() byte {
	return byte(c)
}

func (c Command) String() string {
	switch c {
	case ComSleep:
		return "COM_SLEEP"
	case ComQuit:
		return "COM_QUIT"
	case ComInitDB:
		return "COM_INIT_DB"
	case ComQuery:
		return "COM_QUERY"
	case ComFieldList:
		return "COM_FIELD_LIST"
	case ComCreateDB:
		return "COM_CREATE_DB"
	case ComDropDB:
		return "COM_DROP_DB"
	case ComRefresh:
		return "COM_REFRESH"
	case ComShutdown:
		return "COM_SHUTDOWN"
	case ComStatistics:
		return "COM_STATISTICS"
	case ComProcessInfo:
		return "COM_PROCESS_INFO"
	case ComConnect:
		return "COM_CONNECT"
	case ComProcessKill:
		return "COM_PROCESS_KILL"
	case ComDebug:
		return "COM_DEBUG"
	case ComPing:
		return "COM_PING"
	case ComTime:
		return "COM_TIME"
	case ComDelayedInsert:
		return "COM_DELAYED_INSERT"
	case ComChangeUser:
		return "COM_CHANGE_USER"
	case ComBinlogDump:
		return "COM_BINLOG_DUMP"
	case ComTableDump:
		return "COM_TABLE_DUMP"
	case ComConnectOut:
		return "COM_CONNECT_OUT"
	case ComRegisterSlave:
		return "COM_REGISTER_SLAVE"
	case ComStmtPrepare:
		return "COM_STMT_PREPARE"
	case ComStmtExecute:
		return "COM_STMT_EXECUTE"
	case ComStmtSendLongData:
		return "COM_STMT_SEND_LONG_DATA"
	case ComStmtClose:
		return "COM_STMT_CLOSE"
	case ComStmtReset:
		return "COM_STMT_RESET"
	case ComSetOption:
		return "COM_SET_OPTION"
	case ComStmtFetch:
		return "COM_STMT_FETCH"
	case ComDaemon:
		return "COM_DAEMON"
	case ComBinlogDumpGTID:
		return "COM_BINLOG_DUMP_GTID"
	case ComResetConnection:
		return "COM_RESET_CONNECTION"
	default:
		return "Unknown Command"
	}
}

func NewCmd(cmd Command, data []byte) *Simple {
	data = append([]byte{byte(cmd)}, data...)
	return NewSimple(data)
}

func ParseColumnCount(data []byte) (uint64, error) {
	if len(data) < 5 {
		return 0, ErrPacketData
	}
	buf := bytes.NewBuffer(data[4:])
	columnCount, err := LengthEncodedInteger.Get(buf)
	return columnCount, err
}

func NewColumnCount(count int) (Packet, error) {
	return NewSimple(LengthEncodedInteger.Dump(uint64(count))), nil
}
