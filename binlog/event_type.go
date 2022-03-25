package binlog

type EventType uint8

const (
	EventTypeUnknown EventType = iota
	EventTypeStartV3
	EventTypeQuery
	EventTypeStop
	EventTypeRotate
	EventTypeIntvar
	EventTypeLoad
	EventTypeSlave
	EventTypeCreateFile
	EventTypeAppendBlock
	EventTypeExecLoad
	EventTypeDeleteFile
	EventTypeNewLoad
	EventTypeRand
	EventTypeUserVar
	EventTypeFormatDescription
	EventTypeXid
	EventTypeBeginLoadQuery
	EventTypeExecuteLoadQuery
	EventTypeTableMap
	EventTypeWriteRowsV0
	EventTypeUpdateRowsV0
	EventTypeDeleteRowsV0
	EventTypeWriteRowsV1
	EventTypeUpdateRowsV1
	EventTypeDeleteRowsV1
	EventTypeIncident
	EventTypeHeartbeat
	EventTypeIgnorable
	EventTypeRowsQuery
	EventTypeWriteRowsV2
	EventTypeUpdateRowsV2
	EventTypeDeleteRowsV2
	EventTypeGTID
	EventTypeAnonymousGTID
	EventTypePreviousGTIDs
	EventTypeTransactionContext
	EventTypeViewChange
	EventTypeXAPrepareLog
	EventTypePartialUpdateRows
	EventTypeTransactionPayload

	// EventTypeEnumEnd is end marker, not event type.
	EventTypeEnumEnd
)

func (t EventType) String() string {
	switch t {
	case EventTypeUnknown:
		return "UNKNOWN_EVENT"
	case EventTypeStartV3:
		return "START_EVENT_V3"
	case EventTypeQuery:
		return "QUERY_EVENT"
	case EventTypeStop:
		return "STOP_EVENT"
	case EventTypeRotate:
		return "ROTATE_EVENT"
	case EventTypeIntvar:
		return "ROTATE_EVENT"
	case EventTypeLoad:
		return "LOAD_EVENT"
	case EventTypeSlave:
		return "SLAVE_EVENT"
	case EventTypeCreateFile:
		return "CREATE_FILE_EVENT"
	case EventTypeAppendBlock:
		return "APPEND_BLOCK_EVENT"
	case EventTypeExecLoad:
		return "EXEC_LOAD_EVENT"
	case EventTypeDeleteFile:
		return "DELETE_FILE_EVENT"
	case EventTypeNewLoad:
		return "NEW_LOAD_EVENT"
	case EventTypeRand:
		return "RAND_EVENT"
	case EventTypeUserVar:
		return "USER_VAR_EVENT"
	case EventTypeFormatDescription:
		return "FORMAT_DESCRIPTION_EVENT"
	case EventTypeXid:
		return "XID_EVENT"
	case EventTypeBeginLoadQuery:
		return "BEGIN_LOAD_QUERY_EVENT"
	case EventTypeExecuteLoadQuery:
		return "EXECUTE_LOAD_QUERY_EVENT"
	case EventTypeTableMap:
		return "TABLE_MAP_EVENT"
	case EventTypeWriteRowsV0:
		return "WRITE_ROWS_EVENTv0"
	case EventTypeUpdateRowsV0:
		return "UPDATE_ROWS_EVENTv0"
	case EventTypeDeleteRowsV0:
		return "DELETE_ROWS_EVENTv0"
	case EventTypeWriteRowsV1:
		return "WRITE_ROWS_EVENTv1"
	case EventTypeUpdateRowsV1:
		return "UPDATE_ROWS_EVENTv1"
	case EventTypeDeleteRowsV1:
		return "DELETE_ROWS_EVENTv1"
	case EventTypeIncident:
		return "INCIDENT_EVENT"
	case EventTypeHeartbeat:
		return "HEARTBEAT_EVENT"
	case EventTypeIgnorable:
		return "IGNORABLE_EVENT"
	case EventTypeRowsQuery:
		return "ROWS_QUERY_EVENT"
	case EventTypeWriteRowsV2:
		return "WRITE_ROWS_EVENTv2"
	case EventTypeUpdateRowsV2:
		return "UPDATE_ROWS_EVENTv2"
	case EventTypeDeleteRowsV2:
		return "DELETE_ROWS_EVENTv2"
	case EventTypeGTID:
		return "GTID_EVENT"
	case EventTypeAnonymousGTID:
		return "ANONYMOUS_GTID_EVENT"
	case EventTypePreviousGTIDs:
		return "PREVIOUS_GTIDS_EVENT"
	case EventTypeTransactionContext:
		return "TRANSACTION_CONTEXT_EVENT"
	case EventTypeViewChange:
		return "VIEW_CHANGE_EVENT"
	case EventTypeXAPrepareLog:
		return "XA_PREPARE_LOG_EVENT"
	case EventTypePartialUpdateRows:
		return "PARTIAL_UPDATE_ROWS_EVENT"
	case EventTypeTransactionPayload:
		return "TRANSACTION_PAYLOAD_EVENT"
	default:
		return "unknown event type"
	}
}
