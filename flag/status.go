package flag

import "strings"

type Status uint16

// TODO change to iota
// Status Flags: https://dev.mysql.com/doc/internals/en/status-flags.html
const (
	ServerStatusInTrans            Status = 0x0001
	ServerStatusAutocommit                = 0x0002
	ServerMoreResultsExists               = 0x0008
	ServerStatusNoGoodIndexUsed           = 0x0010
	ServerStatusNoIndexUsed               = 0x0020
	ServerStatusCursorExists              = 0x0040
	ServerStatusLastRowSent               = 0x0080
	ServerStatusDBDropped                 = 0x0100
	ServerStatusNoBackslashEscapes        = 0x0200
	ServerStatusMetadataChanged           = 0x0400
	ServerQueryWasSlow                    = 0x0800
	ServerPsOutParams                     = 0x1000
	ServerStatusInTransReadonly           = 0x2000
	ServerSessionStateChanged             = 0x4000
)

func (s Status) String() string {
	var ss []Status
	s.confirm(ServerStatusInTrans, &ss)
	s.confirm(ServerStatusAutocommit, &ss)
	s.confirm(ServerMoreResultsExists, &ss)
	s.confirm(ServerStatusNoGoodIndexUsed, &ss)
	s.confirm(ServerStatusNoIndexUsed, &ss)
	s.confirm(ServerStatusCursorExists, &ss)
	s.confirm(ServerStatusLastRowSent, &ss)
	s.confirm(ServerStatusDBDropped, &ss)
	s.confirm(ServerStatusNoBackslashEscapes, &ss)
	s.confirm(ServerStatusMetadataChanged, &ss)
	s.confirm(ServerQueryWasSlow, &ss)
	s.confirm(ServerPsOutParams, &ss)
	s.confirm(ServerStatusInTransReadonly, &ss)
	s.confirm(ServerSessionStateChanged, &ss)

	var sb strings.Builder
	sb.WriteByte('[')
	for i, status := range ss {
		if i != 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(status.string())
	}
	sb.WriteByte(']')
	return sb.String()
}

func (s Status) string() string {
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
	case ServerStatusDBDropped:
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
	case ServerSessionStateChanged:
		return "SERVER_SESSION_STATE_CHANGED"
	default:
		return "Unknown Status"
	}
}

func (s Status) confirm(o Status, t *[]Status) {
	if s&o != 0 {
		*t = append(*t, o)
	}
}
