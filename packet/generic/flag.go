package generic

// Status Flags: https://dev.mysql.com/doc/internals/en/status-flags.html
const (
	SERVER_STATUS_IN_TRANS             = 0x0001
	SERVER_STATUS_AUTOCOMMIT           = 0x0002
	SERVER_MORE_RESULTS_EXISTS         = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED   = 0x0010
	SERVER_STATUS_NO_INDEX_USED        = 0x0020
	SERVER_STATUS_CURSOR_EXISTS        = 0x0040
	SERVER_STATUS_LAST_ROW_SENT        = 0x0080
	SERVER_STATUS_DB_DROPPED           = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200
	SERVER_STATUS_METADATA_CHANGED     = 0x0400
	SERVER_QUERY_WAS_SLOW              = 0x0800
	SERVER_PS_OUT_PARAMS               = 0x1000
	SERVER_STATUS_IN_TRANS_READONLY    = 0x2000
	SERVER_SESSION_STATE_CHANGED       = 0x4000
)

// Capability Flags: https://dev.mysql.com/doc/internals/en/capability-flags.html
const (
	CLIENT_LONG_PASSWORD     uint32 = 0x00000001
	CLIENT_FOUND_ROWS               = 0x00000002
	CLIENT_LONG_FLAG                = 0x00000004
	CLIENT_CONNECT_WITH_DB          = 0x00000008
	CLIENT_NO_SCHEMA                = 0x00000010
	CLIENT_COMPRESS                 = 0x00000020
	CLIENT_ODBC                     = 0x00000040
	CLIENT_LOCAL_FILES              = 0x00000080
	CLIENT_IGNORE_SPACE             = 0x00000100
	CLIENT_PROTOCOL_41              = 0x00000200
	CLIENT_INTERACTIVE              = 0x00000400
	CLIENT_SSL                      = 0x00000800
	CLIENT_IGNORE_SIGPIPE           = 0x00001000
	CLIENT_TRANSACTIONS             = 0x00002000
	CLIENT_RESERVED                 = 0x00004000
	CLIENT_SECURE_CONNECTION        = 0x00008000

	CLIENT_MULTI_STATEMENTS               = 0x00010000
	CLIENT_MULTI_RESULTS                  = 0x00020000
	CLIENT_PS_MULTI_RESULTS               = 0x00040000
	CLIENT_PLUGIN_AUTH                    = 0x00080000
	CLIENT_CONNECT_ATTRS                  = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   = 0x00400000
	CLIENT_SESSION_TRACK                  = 0x00800000
	CLIENT_DEPRECATE_EOF                  = 0x01000000
)

type ColumnDefinitionFlag uint16

// Column Definition Flags: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
const (
	NOT_NULL_FLAG ColumnDefinitionFlag = 1 << iota
	PRI_KEY_FLAG
	UNIQUE_KEY_FLAG
	MULTIPLE_KEY_FLAG
	BLOB_FLAG
	UNSIGNED_FLAG
	ZEROFILL_FLAG
	BINARY_FLAG
	ENUM_FLAG
	AUTO_INCREMENT_FLAG
	TIMESTAMP_FLAG
	SET_FLAG
)
