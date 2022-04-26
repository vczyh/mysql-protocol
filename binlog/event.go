package binlog

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
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

func FillEventHeader(header *EventHeader, buf *mysql.Buffer) (err error) {
	// Timestamp
	if header.Timestamp, err = buf.Uint32(); err != nil {
		return err
	}

	// EventType
	b, err := buf.ReadByte()
	if err != nil {
		return err
	}
	header.EventType = EventType(b)

	// ServerId
	if header.ServerId, err = buf.Uint32(); err != nil {
		return err
	}

	// Event size (header, post-header, body)
	if header.EventSize, err = buf.Uint32(); err != nil {
		return err
	}

	// Position of the next event
	if header.LogPos, err = buf.Uint32(); err != nil {
		return err
	}

	// Flags
	u, err := buf.Uint16()
	if err != nil {
		return err
	}
	header.Flags = EventFlag(u)

	return nil
}

func boolToInt(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func hasSignednessType(t packet.TableColumnType) bool {
	switch t {
	case packet.MySQLTypeTiny,
		packet.MySQLTypeShort,
		packet.MySQLTypeInt24,
		packet.MySQLTypeLong,
		packet.MySQLTypeLongLong,
		packet.MySQLTypeYear,
		packet.MySQLTypeFloat,
		packet.MySQLTypeDouble,
		// TODO delete it?
		packet.MySQLTypeDecimal,
		packet.MySQLTypeNewDecimal:
		return true
	default:
		return false
	}
}

func isCharacterType(t packet.TableColumnType) bool {
	switch t {
	case packet.MySQLTypeString,
		// TODO delete it?
		packet.MySQLTypeVarString,
		packet.MySQLTypeVarchar,
		packet.MySQLTypeBlob:
		return true
	default:
		return false
	}
}

func isEnumSetType(t packet.TableColumnType) bool {
	switch t {
	case packet.MySQLTypeEnum,
		packet.MySQLTypeSet:
		return true
	default:
		return false
	}
}

//const (
//	digPerDec1  = 9
//	sizeOfInt32 = 4
//)
//
//var (
//	dig2bytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
//	powers10  = []int{
//		1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000}
//)

//func parseDecimal(data []byte, precision, scale int) (string, error) {
//	intg := precision - scale
//	intg0 := intg / digPerDec1
//	frac0 := scale / digPerDec1
//	intg0x := intg - intg0*digPerDec1
//	frac0x := scale - frac0*digPerDec1
//
//	binSize := intg0*sizeOfInt32 + dig2bytes[intg0x] + frac0*sizeOfInt32 + dig2bytes[frac0x]
//	if len(data) < binSize {
//		return "", io.EOF
//	}
//
//	mask := 0
//	if data[0]&0x80 == 0 {
//		mask = -1
//	}
//
//	data[0] ^= 0x80
//	from := 0
//
//	sb := new(strings.Builder)
//	if mask != 0 {
//		sb.WriteByte('-')
//	}
//
//	if intg0x != 0 {
//		i := dig2bytes[intg0x]
//		var x int
//		switch i {
//		case 1:
//			x = int(data[from])
//		case 2:
//			x = int(binary.BigEndian.Uint16(data))
//		case 3:
//			x = int(uint32(data[2]) | uint32(data[1])<<8 | uint32(data[0])<<16)
//		case 4:
//			x = int(binary.BigEndian.Uint32(data))
//		}
//		from += i
//		x ^= mask
//		if x < 0 || x >= powers10[intg0x+1] {
//			return "", fmt.Errorf("bad format, x exceed: %d, %d", x, powers10[intg0x+1])
//		}
//		if x != 0 {
//
//		}
//	}
//
//	return sb.String(), nil
//}

//func decimalBinSize(precision, scale int) int {
//	intg := precision - scale
//	intg0 := intg / digPerDec1
//	frac0 := scale / digPerDec1
//	intg0x := intg - intg0*digPerDec1
//	frac0x := scale - frac0*digPerDec1
//
//	return intg0*sizeOfInt32 + dig2bytes[intg0x] + frac0*sizeOfInt32 + dig2bytes[frac0x]
//}
