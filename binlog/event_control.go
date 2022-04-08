package binlog

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	UndefinedServerVersion = 999999
)

var (
	ChecksumVersionSplit   = []uint8{5, 6, 1}
	ChecksumVersionProduct = productVersion(ChecksumVersionSplit)
)

type ChecksumAlgorithm uint8

const (
	ChecksumAlgOff ChecksumAlgorithm = iota
	ChecksumAlgCRC32
	ChecksumAlgEnumEnd
	ChecksumAlgUndefined ChecksumAlgorithm = 255
)

func (a ChecksumAlgorithm) String() string {
	switch a {
	case ChecksumAlgOff:
		return "BINLOG_CHECKSUM_ALG_OFF"
	case ChecksumAlgCRC32:
		return "BINLOG_CHECKSUM_ALG_CRC32"
	case ChecksumAlgUndefined:
		return "BINLOG_CHECKSUM_ALG_UNDEF"
	default:
		return "unknown checksum algorithm"
	}
}

type RotateEvent struct {
	EventHeader
	Position uint64
	Name     string
}

func ParseRotateEvent(data []byte) (*RotateEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(RotateEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Start position of the next binlog
	e.Position = packet.FixedLengthInteger.Get(buf.Next(8))

	// Name of the next binlog
	e.Name = string(buf.Bytes())

	return e, nil
}

func (e *RotateEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Position: %d\n", e.Position)
	fmt.Fprintf(sb, "Name: %s\n", e.Name)

	return sb.String()
}

type FormatDescriptionEvent struct {
	EventHeader
	BinlogVersion   uint16
	ServerVersion   string
	CreateTimestamp uint32
	HeaderLen       uint8
	//PostHeaderLens  []uint8
	PostHeaderLenMap map[EventType]uint8
	ChecksumAlg      ChecksumAlgorithm
}

func ParseFormatDescriptionEvent(data []byte) (*FormatDescriptionEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(FormatDescriptionEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Binlog version
	e.BinlogVersion = uint16(packet.FixedLengthInteger.Get(buf.Next(2)))

	// MySQL server version
	e.ServerVersion = strings.TrimRight(string(buf.Next(50)), "\u0000")

	// Create timestamp
	e.CreateTimestamp = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Event header length
	b, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.HeaderLen = b

	// Post header lengths and checksum algorithm
	var postHeaderLens []uint8
	if version := productVersion(splitServerVersion(e.ServerVersion)); version >= ChecksumVersionProduct {
		postHeaderLens = buf.Next(buf.Len() - 1)
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		e.ChecksumAlg = ChecksumAlgorithm(b)
	} else {
		postHeaderLens = buf.Bytes()
		e.ChecksumAlg = ChecksumAlgUndefined
	}

	e.PostHeaderLenMap = make(map[EventType]uint8, len(postHeaderLens))
	for i, l := range postHeaderLens {
		e.PostHeaderLenMap[EventType(i+1)] = l
	}

	return e, nil
}

func (e *FormatDescriptionEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Binlog version: %d\n", e.BinlogVersion)
	fmt.Fprintf(sb, "Server version: %s\n", e.ServerVersion)
	fmt.Fprintf(sb, "Create timestamp: %s\n", time.Unix(int64(e.CreateTimestamp), 0).Format(time.RFC3339))
	fmt.Fprintf(sb, "Header length: %d\n", e.HeaderLen)
	fmt.Fprintf(sb, "Event types number: %d\n", len(e.PostHeaderLenMap))

	var postHeaderLens []string
	for k, v := range e.PostHeaderLenMap {
		postHeaderLens = append(postHeaderLens, fmt.Sprintf("%s:%d", k, v))
	}
	fmt.Fprintf(sb, "Post header lengths: [%s]\n", strings.Join(postHeaderLens, ", "))

	return sb.String()
}

type PreviousGTIDsEvent struct {
	EventHeader
	GTIDSet string
}

func ParsePreviousGTIDsEvent(data []byte) (*PreviousGTIDsEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(PreviousGTIDsEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// GTID set
	sidNum := packet.FixedLengthInteger.Get(buf.Next(8))
	set := make([]string, sidNum)
	for i := 0; i < int(sidNum); i++ {
		sid := hex.EncodeToString(buf.Next(4)) +
			"-" + hex.EncodeToString(buf.Next(2)) +
			"-" + hex.EncodeToString(buf.Next(2)) +
			"-" + hex.EncodeToString(buf.Next(2)) +
			"-" + hex.EncodeToString(buf.Next(6))

		intervalNum := packet.FixedLengthInteger.Get(buf.Next(8))
		intervals := make([]string, intervalNum)
		for j := 0; j < int(intervalNum); j++ {
			start := packet.FixedLengthInteger.Get(buf.Next(8))
			end := packet.FixedLengthInteger.Get(buf.Next(8))
			if start == end-1 {
				intervals[j] = strconv.FormatUint(start, 10)
			} else {
				intervals[j] = strconv.FormatUint(start, 10) + "-" + strconv.FormatUint(end-1, 10)
			}
		}
		set[i] = sid + ":" + strings.Join(intervals, ":")
	}
	e.GTIDSet = strings.Join(set, ",")

	return e, nil
}

func (e *PreviousGTIDsEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "GTID Set: %s\n", e.GTIDSet)

	return sb.String()
}

type GTIDEvent struct {
	EventHeader
	FLags uint8
	SID   []byte
	GNO   uint64

	LogicalTimestampTypeCode uint8
	LastCommitted            uint64
	SequenceNumber           uint64

	ImmediateCommitTimestamp uint64
	OriginalCommitTimestamp  uint64

	TransactionLength uint64

	ImmediateServerVersion uint32
	OriginalServerVersion  uint32
}

func ParseGTIDEvent(data []byte) (*GTIDEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(GTIDEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Flags
	b, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.FLags = b

	// SID
	e.SID = buf.Next(16)
	e.GNO = packet.FixedLengthInteger.Get(buf.Next(8))

	// Lc type code
	b, err = buf.ReadByte()
	if err != nil {
		if err == io.EOF {
			return e, nil
		}
		return nil, err
	}
	e.LogicalTimestampTypeCode = b

	if e.LogicalTimestampTypeCode == 2 {
		// Last committed
		e.LastCommitted = packet.FixedLengthInteger.Get(buf.Next(8))
		// Sequence number
		e.SequenceNumber = packet.FixedLengthInteger.Get(buf.Next(8))

		// Fetch the timestamps used to monitor replication lags with respect to
		// the immediate master and the server that originated this transaction.
		// Check that the timestamps exist before reading. Note that a master
		// older than MySQL-5.8 will NOT send these timestamps. We should be
		// able to ignore these fields in this case.
		if buf.Len() < 7 {
			return e, nil
		}
		e.ImmediateCommitTimestamp = packet.FixedLengthInteger.Get(buf.Next(7))
		if e.ImmediateCommitTimestamp&(1<<55) != 0 {
			e.ImmediateCommitTimestamp &^= 1 << 55
			e.OriginalCommitTimestamp = packet.FixedLengthInteger.Get(buf.Next(7))
		} else {
			// The transaction originated in the previous server.
			e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
		}

		// Transaction length
		if buf.Len() < 1 {
			return e, nil
		}
		if e.TransactionLength, err = packet.LengthEncodedInteger.Get(buf); err != nil {
			return nil, err
		}

		// Fetch the original/immediate_server_version. Set it to
		// UNDEFINED_SERVER_VERSION if no version can be fetched.
		e.OriginalServerVersion = UndefinedServerVersion
		e.ImmediateServerVersion = UndefinedServerVersion
		if buf.Len() < 4 {
			return e, nil
		}
		e.ImmediateServerVersion = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))
		if e.ImmediateServerVersion&uint32(1<<31) != 0 {
			e.ImmediateServerVersion &^= 1 << 31
			e.OriginalServerVersion = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))
		} else {
			e.OriginalServerVersion = e.ImmediateServerVersion
		}
	}

	return e, nil
}

func (e *GTIDEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "FLags: %d\n", e.FLags)
	fmt.Fprintf(sb, "SID: %s\n", hex.EncodeToString(e.SID))
	fmt.Fprintf(sb, "GNO: %d\n", e.GNO)
	fmt.Fprintf(sb, "Logical timestamp type code: %d\n", e.LogicalTimestampTypeCode)
	fmt.Fprintf(sb, "Last committed: %d\n", e.LastCommitted)
	fmt.Fprintf(sb, "Sequence number: %d\n", e.SequenceNumber)
	fmt.Fprintf(sb, "Immediate commit timestamp: %s\n", time.Unix(0, int64(e.ImmediateCommitTimestamp*1e3)).Format(time.RFC3339Nano))
	fmt.Fprintf(sb, "Original commit timestamp: %s\n", time.Unix(0, int64(e.OriginalCommitTimestamp*1e3)).Format(time.RFC3339Nano))
	fmt.Fprintf(sb, "Transaction length: %d\n", e.TransactionLength)
	fmt.Fprintf(sb, "Immediate server version: %d\n", e.ImmediateServerVersion)
	fmt.Fprintf(sb, "Original server version: %d\n", e.OriginalServerVersion)

	return sb.String()
}

func splitServerVersion(version string) []uint8 {
	versionSplit := make([]uint8, 3)
	r := regexp.MustCompile(`^(\d+)`)

	split := strings.Split(version, ".")
	for i, s := range split {
		m := r.FindString(s)
		if m == "" {
			return []uint8{0, 0, 0}
		}

		num, err := strconv.Atoi(m)
		if err != nil || num >= 256 {
			return []uint8{0, 0, 0}
		}
		versionSplit[i] = uint8(num)
	}

	return versionSplit
}

func productVersion(versionSplit []uint8) int {
	sum := int(versionSplit[0])*256 + int(versionSplit[1])
	sum = sum*256 + int(versionSplit[2])
	return sum
}
