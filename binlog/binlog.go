package binlog

import "strings"

type DumpFlag uint16

const (
	DumpFlagNonBlock DumpFlag = 1 << iota
	DumpFlagThroughPosition
	DumpFlagThroughGTID
)

func (b DumpFlag) String() string {
	switch b {
	case DumpFlagNonBlock:
		return "BINLOG_DUMP_NON_BLOCK"
	case DumpFlagThroughPosition:
		return "BINLOG_THROUGH_POSITION"
	case DumpFlagThroughGTID:
		return "BINLOG_THROUGH_GTID"
	default:
		return "unknown binlog flag"
	}
}

type EventFlag uint16

//https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
const (
	EventFlagBinlogInUse EventFlag = 1 << iota
	EventFlagForcedRotate
	EventFlagThreadSpecific
	EventFlagSuppressUse
	EventFlagUpdateTableMapVersion
	EventFlagArtificial
	EventFlagRelayLog
	EventFlagIgnorable
	EventFlagNoFilter
	EventFlagMtsIsolate
)

func (be EventFlag) String() string {
	var bes []EventFlag
	be.confirm(EventFlagBinlogInUse, &bes)
	be.confirm(EventFlagForcedRotate, &bes)
	be.confirm(EventFlagThreadSpecific, &bes)
	be.confirm(EventFlagSuppressUse, &bes)
	be.confirm(EventFlagUpdateTableMapVersion, &bes)
	be.confirm(EventFlagArtificial, &bes)
	be.confirm(EventFlagRelayLog, &bes)
	be.confirm(EventFlagIgnorable, &bes)
	be.confirm(EventFlagNoFilter, &bes)
	be.confirm(EventFlagMtsIsolate, &bes)

	sb := new(strings.Builder)
	sb.WriteByte('[')
	for i, status := range bes {
		if i != 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(status.string())
	}
	sb.WriteByte(']')

	return sb.String()
}

func (be EventFlag) string() string {
	switch be {
	case EventFlagBinlogInUse:
		return "LOG_EVENT_BINLOG_IN_USE"
	case EventFlagForcedRotate:
		return "LOG_EVENT_FORCED_ROTATE"
	case EventFlagThreadSpecific:
		return "LOG_EVENT_THREAD_SPECIFIC"
	case EventFlagSuppressUse:
		return "LOG_EVENT_SUPPRESS_USE"
	case EventFlagUpdateTableMapVersion:
		return "LOG_EVENT_UPDATE_TABLE_MAP_VERSION"
	case EventFlagArtificial:
		return "LOG_EVENT_ARTIFICIAL"
	case EventFlagRelayLog:
		return "LOG_EVENT_RELAY_LOG"
	case EventFlagIgnorable:
		return "LOG_EVENT_IGNORABLE"
	case EventFlagNoFilter:
		return "LOG_EVENT_NO_FILTER"
	case EventFlagMtsIsolate:
		return "LOG_EVENT_MTS_ISOLATE"
	default:
		return "unknown binlog event flag"
	}
}

func (s EventFlag) confirm(o EventFlag, t *[]EventFlag) {
	if s&o != 0 {
		*t = append(*t, o)
	}
}

type QueryEventStatusVars uint8

const (
	QueryStatusVarsFlags2 QueryEventStatusVars = iota
	QueryStatusVarsSQLMode
	QueryStatusVarsCatalog
	QueryStatusVarsAutoIncrement
	QueryStatusVarsCharset
	QueryStatusVarsTimeZone
	QueryStatusVarsCatalogNz
	QueryStatusVarsLcTimeNames
	QueryStatusVarsCharsetDatabase
	QueryStatusVarsTableMapForUpdate
	QueryStatusVarsMasterDataWritten
	QueryStatusVarsInvoker
	QueryStatusVarsUpdatedDBNames
	QueryStatusVarsMicroseconds
	QueryStatusVarsCommitTS
	QueryStatusVarsCommitTS2
	QueryStatusVarsExplicitDefaultsForTimestamp
	QueryStatusVarsDDLLoggedWithXid
	QueryStatusVarsDefaultCollationForUtf8mb4
	QueryStatusVarsSQLRequirePrimaryKey
	QueryStatusVarsDefaultTableEncryption
)

type Ternary uint8

const (
	TernaryUnset Ternary = iota
	TernaryOff
	TernaryOn
)

type Option uint32

const (
	OptionAutoIsNull          Option = 1 << 14
	OptionNotAutocommit       Option = 1 << 19
	OptionNoForeignKeyChecks  Option = 1 << 26
	OptionRelaxedUniqueChecks Option = 1 << 27
)

type SQLMode uint64

const (
	SQLModeRealAsFloat = 1 << iota
	SQLModePipesAsConcat
	SQLModeANSIQuotes
	SQLModeIgnoreSpace
	SQLModeNotUsed
	SQLModeOnlyFullGroupBy
	SQLModeNoUnsignedSubtraction
	SQLModeNoDirInCreate
	SQLModePostgreSQL
	SQLModeOracle
	SQLModeMSSQL
	SQLModeDB2
	SQLModeMaxDB
	SQLModeNoKeyOptions
	SQLModeNoTableOptions
	SQLModeNoFieldOptions
	SQLModeMySQL323
	SQLModeMySQL40
	SQLModeANSI
	SQLModeNoAutoValueOnZero
	SQLModeNoBackslashEscapes
	SQLModeStrictTransTables
	SQLModeStrictAllTables
	SQLModeNoZeroInDate
	SQLModeNoZeroDate
	SQLModeInvalidDates
	SQLModeErrorForDivisionByZero
	SQLModeTraditional
	SQLModeNoAutoCreateUser
	SQLModeHighNotPrecedence
	SQLModeNoEngineSubstitution
	SQLModePadCharToFullLength
)
