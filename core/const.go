package core

type TableColumnType uint8

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
// https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
const (
	MySQLTypeDecimal TableColumnType = iota
	MySQLTypeTiny
	MySQLTypeShort
	MySQLTypeLong
	MySQLTypeFloat
	MySQLTypeDouble
	MySQLTypeNull
	MySQLTypeTimestamp
	MySQLTypeLongLong
	MySQLTypeInt24
	MySQLTypeDate
	MySQLTypeTime
	MySQLTypeDatetime
	MySQLTypeYear
	MySQLTypeNewDate
	MySQLTypeVarchar
	MySQLTypeBit
	MySQLTypeTimestamp2
	MySQLTypeDatetime2
	MySQLTypeTime2
	MySQLTypeNewDecimal = iota + 0xe2
	MySQLTypeEnum
	MySQLTypeSet
	MySQLTypeTinyBlob
	MySQLTypeMediumBlob
	MySQLTypeLongBlob
	MySQLTypeBlob
	MySQLTypeVarString
	MySQLTypeString
	MySQLTypeGeometry
)

func (t TableColumnType) String() string {
	switch t {
	case MySQLTypeDecimal:
		return "MYSQL_TYPE_DECIMAL"
	case MySQLTypeTiny:
		return "MYSQL_TYPE_TINY"
	case MySQLTypeShort:
		return "MYSQL_TYPE_SHORT"
	case MySQLTypeLong:
		return "MYSQL_TYPE_LONG"
	case MySQLTypeFloat:
		return "MYSQL_TYPE_FLOAT"
	case MySQLTypeDouble:
		return "MYSQL_TYPE_DOUBLE"
	case MySQLTypeNull:
		return "MYSQL_TYPE_NULL"
	case MySQLTypeTimestamp:
		return "MYSQL_TYPE_TIMESTAMP"
	case MySQLTypeLongLong:
		return "MYSQL_TYPE_LONGLONG"
	case MySQLTypeInt24:
		return "MYSQL_TYPE_INT24"
	case MySQLTypeDate:
		return "MYSQL_TYPE_DATE"
	case MySQLTypeTime:
		return "MYSQL_TYPE_TIME"
	case MySQLTypeDatetime:
		return "MYSQL_TYPE_DATETIME"
	case MySQLTypeYear:
		return "MYSQL_TYPE_YEAR"
	case MySQLTypeNewDate:
		return "MYSQL_TYPE_NEWDATE"
	case MySQLTypeVarchar:
		return "MYSQL_TYPE_VARCHAR"
	case MySQLTypeBit:
		return "MYSQL_TYPE_BIT"
	case MySQLTypeTimestamp2:
		return "MYSQL_TYPE_TIMESTAMP2"
	case MySQLTypeDatetime2:
		return "MYSQL_TYPE_DATETIME2"
	case MySQLTypeTime2:
		return "MYSQL_TYPE_TIME2"
	case MySQLTypeNewDecimal:
		return "MYSQL_TYPE_NEWDECIMAL"
	case MySQLTypeEnum:
		return "MYSQL_TYPE_ENUM"
	case MySQLTypeSet:
		return "MYSQL_TYPE_SET"
	case MySQLTypeTinyBlob:
		return "MYSQL_TYPE_TINY_BLOB"
	case MySQLTypeMediumBlob:
		return "MYSQL_TYPE_MEDIUM_BLOB"
	case MySQLTypeLongBlob:
		return "MYSQL_TYPE_LONG_BLOB"
	case MySQLTypeBlob:
		return "MYSQL_TYPE_BLOB"
	case MySQLTypeVarString:
		return "MYSQL_TYPE_VAR_STRING"
	case MySQLTypeString:
		return "MYSQL_TYPE_STRING"
	case MySQLTypeGeometry:
		return "MYSQL_TYPE_GEOMETRY"
	default:
		return "Unknown TableColumnType"
	}
}

type StatusFlag uint16

// TODO change to iota
// Status Flags: https://dev.mysql.com/doc/internals/en/status-flags.html
const (
	ServerStatusInTrans            StatusFlag = 0x0001
	ServerStatusAutocommit                    = 0x0002
	ServerMoreResultsExists                   = 0x0008
	ServerStatusNoGoodIndexUsed               = 0x0010
	ServerStatusNoIndexUsed                   = 0x0020
	ServerStatusCursorExists                  = 0x0040
	ServerStatusLastRowSent                   = 0x0080
	ServerStatusDbDropped                     = 0x0100
	ServerStatusNoBackslashEscapes            = 0x0200
	ServerStatusMetadataChanged               = 0x0400
	ServerQueryWasSlow                        = 0x0800
	ServerPsOutParams                         = 0x1000
	ServerStatusInTransReadonly               = 0x2000
	ServerSessionStateChanged                 = 0x4000
)

func (s StatusFlag) String() string {
	switch s {
	case ServerStatusInTrans:
		return "SERVER_STATUS_IN_TRANS"
	case ServerStatusAutocommit:
		return "SERVER_STATUS_AUTOCOMMIT"
	case ServerMoreResultsExists:
		return "SERVER_MORE_RESULTS_EXISTS"
	case ServerStatusNoGoodIndexUsed:
		return "SERVER_STATUS_NO_GOOD_INDEX_USED"
	case ServerStatusNoIndexUsed:
		return "SERVER_STATUS_NO_INDEX_USED"
	case ServerStatusCursorExists:
		return "SERVER_STATUS_CURSOR_EXISTS"
	case ServerStatusLastRowSent:
		return "SERVER_STATUS_LAST_ROW_SENT"
	case ServerStatusDbDropped:
		return "SERVER_STATUS_DB_DROPPED"
	case ServerStatusNoBackslashEscapes:
		return "SERVER_STATUS_NO_BACKSLASH_ESCAPES"
	case ServerStatusMetadataChanged:
		return "SERVER_STATUS_METADATA_CHANGED"
	case ServerQueryWasSlow:
		return "SERVER_QUERY_WAS_SLOW"
	case ServerPsOutParams:
		return "SERVER_PS_OUT_PARAMS"
	case ServerStatusInTransReadonly:
		return "SERVER_STATUS_IN_TRANS_READONLY"
	default:
		return "Unknown StatusFlag"
	}
}

type CapabilityFlag uint32

// TODO change to iota
// Capability Flags: https://dev.mysql.com/doc/internals/en/capability-flags.html
//const (
//	ClientLongPassword               CapabilityFlag = 0x00000001
//	ClientFoundRows                                 = 0x00000002
//	ClientLongFlag                                  = 0x00000004
//	ClientConnectWithDB                             = 0x00000008
//	ClientNoSchema                                  = 0x00000010
//	ClientCompress                                  = 0x00000020
//	ClientODBC                                      = 0x00000040
//	ClientLocalFiles                                = 0x00000080
//	ClientIgnoreSpace                               = 0x00000100
//	ClientProtocol41                                = 0x00000200
//	ClientInteractive                               = 0x00000400
//	ClientSSL                                       = 0x00000800
//	ClientIgnoreSigpipe                             = 0x00001000
//	ClientTransactions                              = 0x00002000
//	ClientReserved                                  = 0x00004000
//	ClientSecureConnection                          = 0x00008000
//	ClientMultiStatements                           = 0x00010000
//	ClientMultiResults                              = 0x00020000
//	ClientPsMultiResults                            = 0x00040000
//	ClientPluginAuth                                = 0x00080000
//	ClientConnectAttrs                              = 0x00100000
//	ClientPluginAuthLenencClientData                = 0x00200000
//	ClientCanHandleExpiredPasswords                 = 0x00400000
//	ClientSessionTrack                              = 0x00800000
//	ClientDeprecateEOF                              = 0x01000000
//)

const (
	ClientLongPassword CapabilityFlag = 1 << iota
	ClientFoundRows
	ClientLongFlag
	ClientConnectWithDB
	ClientNoSchema
	ClientCompress
	ClientODBC
	ClientLocalFiles
	ClientIgnoreSpace
	ClientProtocol41
	ClientInteractive
	ClientSSL
	ClientIgnoreSigpipe
	ClientTransactions
	ClientReserved
	ClientSecureConnection
	ClientMultiStatements
	ClientMultiResults
	ClientPsMultiResults
	ClientPluginAuth
	ClientConnectAttrs
	ClientPluginAuthLenencClientData
	ClientCanHandleExpiredPasswords
	ClientSessionTrack
	ClientDeprecateEOF
)

func (c CapabilityFlag) String() string {
	switch c {
	case ClientLongPassword:
		return "CLIENT_LONG_PASSWORD"
	case ClientFoundRows:
		return "CLIENT_FOUND_ROWS"
	case ClientLongFlag:
		return "CLIENT_LONG_FLAG"
	case ClientConnectWithDB:
		return "CLIENT_CONNECT_WITH_DB"
	case ClientNoSchema:
		return "CLIENT_NO_SCHEMA"
	case ClientCompress:
		return "CLIENT_COMPRESS"
	case ClientODBC:
		return "CLIENT_ODBC"
	case ClientLocalFiles:
		return "CLIENT_LOCAL_FILES"
	case ClientIgnoreSpace:
		return "CLIENT_IGNORE_SPACE"
	case ClientProtocol41:
		return "CLIENT_PROTOCOL_41"
	case ClientInteractive:
		return "CLIENT_INTERACTIVE"
	case ClientSSL:
		return "CLIENT_SSL"
	case ClientIgnoreSigpipe:
		return "CLIENT_IGNORE_SIGPIPE"
	case ClientTransactions:
		return "CLIENT_TRANSACTIONS"
	case ClientReserved:
		return "CLIENT_RESERVED"
	case ClientSecureConnection:
		return "CLIENT_SECURE_CONNECTION"
	case ClientMultiStatements:
		return "CLIENT_MULTI_STATEMENTS"
	case ClientMultiResults:
		return "CLIENT_MULTI_RESULTS"
	case ClientPsMultiResults:
		return "CLIENT_PS_MULTI_RESULTS"
	case ClientPluginAuth:
		return "CLIENT_PLUGIN_AUTH"
	case ClientConnectAttrs:
		return "CLIENT_CONNECT_ATTRS"
	case ClientPluginAuthLenencClientData:
		return "CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"
	case ClientCanHandleExpiredPasswords:
		return "CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS"
	case ClientSessionTrack:
		return "CLIENT_SESSION_TRACK"
	case ClientDeprecateEOF:
		return "CLIENT_DEPRECATE_EOF"
	default:
		return "Unknown CapabilityFlag"
	}
}

type ColumnDefinitionFlag uint16

// Column Definition Flags: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
const (
	NotNullFlag ColumnDefinitionFlag = 1 << iota
	PriKeyFlag
	UniqueKeyFlag
	MultipleKeyFlag
	BlobFlag
	UnsignedFlag
	ZerofillFlag
	BinaryFlag
	EnumFlag
	AutoIncrementFlag
	TimestampFlag
	SetFlag
)

func (cd ColumnDefinitionFlag) String() string {
	switch cd {
	case NotNullFlag:
		return "NOT_NULL_FLAG"
	case PriKeyFlag:
		return "PRI_KEY_FLAG"
	case UniqueKeyFlag:
		return "UNIQUE_KEY_FLAG"
	case MultipleKeyFlag:
		return "MULTIPLE_KEY_FLAG"
	case BlobFlag:
		return "BLOB_FLAG"
	case UnsignedFlag:
		return "UNSIGNED_FLAG"
	case ZerofillFlag:
		return "ZEROFILL_FLAG"
	case BinaryFlag:
		return "BINARY_FLAG"
	case EnumFlag:
		return "ENUM_FLAG"
	case AutoIncrementFlag:
		return "AUTO_INCREMENT_FLAG"
	case TimestampFlag:
		return "TIMESTAMP_FLAG"
	case SetFlag:
		return "SET_FLAG"
	default:
		return "Unknown ColumnDefinitionFlag"
	}
}

type Command uint8

// https://dev.mysql.com/doc/internals/en/command-phase.html
const (
	ComSleep            Command = 0x00
	ComQuit                     = 0x01
	ComInitDB                   = 0x02
	ComQuery                    = 0x03
	ComFieldList                = 0x04
	ComCreateDB                 = 0x05
	ComDropDB                   = 0x06
	ComRefresh                  = 0x07
	ComShutdown                 = 0x08
	ComStatistics               = 0x09
	ComProcessInfo              = 0x0a
	ComConnect                  = 0x0b
	ComProcessKill              = 0x0c
	ComDebug                    = 0x0d
	ComPing                     = 0x0e
	ComTime                     = 0x0f
	ComDelayedInsert            = 0x10
	ComChangeUser               = 0x11
	ComBinlogDump               = 0x12
	ComTableDump                = 0x13
	ComConnectOut               = 0x14
	ComRegisterSlave            = 0x15
	ComStmtPrepare              = 0x16
	ComStmtExecute              = 0x17
	ComStmtSendLongData         = 0x18
	ComStmtClose                = 0x19
	ComStmtReset                = 0x1a
	ComSetOption                = 0x1b
	ComStmtFetch                = 0x1c
	ComDaemon                   = 0x1d
	ComBinlogDumpGTID           = 0x1e
	ComResetConnection          = 0x1f
)

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
