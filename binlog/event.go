package binlog

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet"
	"strings"
	"time"
)

var (
	ErrInvalidData = errors.New("event: invalid data")
)

type Event interface {
	fmt.Stringer
	//packet.Packet
}

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     EventFlag
}

//func (h *EventHeader) Dump(capabilities flag.Capability) ([]byte, error) {
//	// TODO
//	panic("implement")
//}

func (h *EventHeader) String() string {
	sb := new(strings.Builder)

	fmt.Fprintf(sb, "### %s ###\n", h.EventType.String())
	fmt.Fprintf(sb, "Timestamp: %s\n", time.Unix(int64(h.Timestamp), 0).Format(time.RFC3339))
	fmt.Fprintf(sb, "Server id: %d\n", h.ServerId)
	fmt.Fprintf(sb, "Event size: %d\n", h.EventSize)
	fmt.Fprintf(sb, "Log position: %d\n", h.LogPos)
	fmt.Fprintf(sb, "Flags: %s\n", h.Flags.String())

	return sb.String()
}

func FillEventHeader(header *EventHeader, buf *bytes.Buffer) error {
	// Timestamp
	header.Timestamp = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// EventType
	b, err := buf.ReadByte()
	if err != nil {
		return err
	}
	header.EventType = EventType(b)

	// ServerId
	header.ServerId = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Event size (header, post-header, body)
	header.EventSize = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Position of the next event
	header.LogPos = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Flags
	header.Flags = EventFlag(packet.FixedLengthInteger.Get(buf.Next(2)))

	return nil
}

func boolToInt(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
