package binlog

import (
	"fmt"
)

type Parser struct {
	fde *FormatDescriptionEvent

	tableMap map[uint64]*TableMapEvent
}

func NewParser() *Parser {
	p := new(Parser)
	p.tableMap = make(map[uint64]*TableMapEvent)
	return p
}

// ParseEvent performs binary data to event.
// Param data contains CommonHeader, PostHeader, Payload and Checksum.
func (p *Parser) ParseEvent(data []byte) (Event, error) {
	b := data[:len(data)-4]
	eventType := EventType(data[4])

	// TODO checksum

	switch eventType {
	case EventTypeRotate:
		return ParseRotateEvent(b)
	case EventTypeStop:
		return ParseStopEvent(b)
	case EventTypeFormatDescription:
		e, err := ParseFormatDescriptionEvent(b)
		if err != nil {
			return nil, err
		}
		p.fde = e
		return e, nil
	case EventTypePreviousGTIDs:
		return ParsePreviousGTIDsEvent(b)
	case EventTypeGTID:
		return ParseGTIDEvent(b)
	case EventTypeXid:
		return ParseXidEvent(b)
	case EventTypeUserVar:
		return ParseUserVarEvent(b)
	case EventTypeIntvar:
		return ParseIntVarEvent(b)
	case EventTypeIncident:
		return ParseIncidentEvent(b)
	}

	if p.fde == nil {
		return nil, fmt.Errorf("FormatDescriptionEvent is nil")
	}

	switch eventType {
	case EventTypeQuery:
		return ParseQueryEvent(b, p.fde)
	case EventTypeTableMap:
		table, err := ParseTableMapEvent(b, p.fde)
		if err != nil {
			return nil, err
		}
		p.tableMap[table.TableId] = table
		return table, err
	case EventTypeWriteRowsV2, EventTypeDeleteRowsV2, EventTypeUpdateRowsV2:
		// TODO partial event
		return ParseRowsEvent(b, p.fde, p)
	default:
		return nil, fmt.Errorf("unsupported event type")
	}
}

func (p *Parser) TableMapEvent(tableId uint64) (*TableMapEvent, error) {
	table, ok := p.tableMap[tableId]
	if !ok {
		return nil, fmt.Errorf("TableMap for %d not found", tableId)
	}
	return table, nil
}
