package flag

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
