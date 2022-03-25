package binlog

import "fmt"

type Parser struct {
	fde *FormatDescriptionEvent
}

func NewParser() *Parser {
	return &Parser{}
}

// ParseEvent performs binary data to event.
// data contains CommonHeader, PostHeader, Payload and Checksum.
func (p *Parser) ParseEvent(data []byte) (Event, error) {
	b := data[:len(data)-4]
	eventType := EventType(data[4])

	// TODO checksum

	switch eventType {
	case EventTypeRotate:
		return ParseRotateEvent(b)
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
	}

	if p.fde == nil {
		return nil, fmt.Errorf("FormatDescriptionEvent is nil")
	}

	switch eventType {
	case EventTypeQuery:
		return ParseQueryEvent(b, p.fde)
	default:
		return nil, fmt.Errorf("unsupported event type")
	}
}
