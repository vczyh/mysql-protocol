package binlog

import (
	"encoding/hex"
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
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

func ParseRotateEvent(data []byte) (e *RotateEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(RotateEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Start position of the next binlog.
	if e.Position, err = buf.Uint64(); err != nil {
		return nil, err
	}

	// Name of the next binlog.
	e.Name = buf.String()

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

func ParseFormatDescriptionEvent(data []byte) (e *FormatDescriptionEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(FormatDescriptionEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Binlog version
	if e.BinlogVersion, err = buf.Uint16(); err != nil {
		return nil, err
	}

	// MySQL server version
	next, err := buf.Next(50)
	if err != nil {
		return nil, err
	}
	e.ServerVersion = strings.TrimRight(string(next), "\u0000")

	// Create timestamp
	if e.CreateTimestamp, err = buf.Uint32(); err != nil {
		return nil, err
	}

	// Event header length
	if e.HeaderLen, err = buf.Uint8(); err != nil {
		return nil, err
	}

	// Post header lengths and checksum algorithm
	var postHeaderLens []uint8
	if version := productVersion(splitServerVersion(e.ServerVersion)); version >= ChecksumVersionProduct {
		if postHeaderLens, err = buf.Next(buf.Len() - 1); err != nil {
			return nil, err
		}
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

func ParsePreviousGTIDsEvent(data []byte) (e *PreviousGTIDsEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(PreviousGTIDsEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// GTID set
	sidNum, err := buf.Uint64()
	if err != nil {
		return nil, err
	}
	set := make([]string, sidNum)
	for i := 0; i < int(sidNum); i++ {
		if buf.Len() < 16 {
			return nil, io.EOF
		}

		sid := make([]string, 5)
		next, _ := buf.Next(4)
		sid[0] = hex.EncodeToString(next)
		next, _ = buf.Next(2)
		sid[1] = hex.EncodeToString(next)
		next, _ = buf.Next(2)
		sid[2] = hex.EncodeToString(next)
		next, _ = buf.Next(2)
		sid[3] = hex.EncodeToString(next)
		next, _ = buf.Next(6)
		sid[4] = hex.EncodeToString(next)

		intervalNum, err := buf.Uint64()
		if err != nil {
			return nil, err
		}
		intervals := make([]string, intervalNum)
		for j := 0; j < int(intervalNum); j++ {
			start, err := buf.Uint64()
			if err != nil {
				return nil, err
			}

			end, err := buf.Uint64()
			if err != nil {
				return nil, err
			}

			if start == end-1 {
				intervals[j] = strconv.FormatUint(start, 10)
			} else {
				intervals[j] = strconv.FormatUint(start, 10) + "-" + strconv.FormatUint(end-1, 10)
			}
		}
		set[i] = strings.Join(sid, "-") + ":" + strings.Join(intervals, ":")
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

func ParseGTIDEvent(data []byte) (e *GTIDEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(GTIDEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Flags
	if e.FLags, err = buf.Uint8(); err != nil {
		return nil, err
	}

	// SID
	if e.SID, err = buf.Next(16); err != nil {
		return nil, err
	}

	if e.GNO, err = buf.Uint64(); err != nil {
		return nil, err
	}

	// TODO needed?
	if buf.Len() == 0 {
		return e, nil
	}

	// Lc type code
	e.LogicalTimestampTypeCode, err = buf.Uint8()
	if err != nil {
		return nil, err
	}

	if e.LogicalTimestampTypeCode == 2 {
		// Last committed
		if e.LastCommitted, err = buf.Uint64(); err != nil {
			return nil, err
		}

		// Sequence number
		if e.SequenceNumber, err = buf.Uint64(); err != nil {
			return nil, err
		}

		// Fetch the timestamps used to monitor replication lags with respect to
		// the immediate master and the server that originated this transaction.
		// Check that the timestamps exist before reading. Note that a master
		// older than MySQL-5.8 will NOT send these timestamps. We should be
		// able to ignore these fields in this case.
		if buf.Len() < 7 {
			return e, nil
		}
		if e.ImmediateCommitTimestamp, err = buf.Uint56(); err != nil {
			return nil, err
		}
		if e.ImmediateCommitTimestamp&(1<<55) != 0 {
			e.ImmediateCommitTimestamp &^= 1 << 55
			if e.OriginalCommitTimestamp, err = buf.Uint56(); err != nil {
				return nil, err
			}
		} else {
			// The transaction originated in the previous server.
			e.OriginalCommitTimestamp = e.ImmediateCommitTimestamp
		}

		// Transaction length
		if buf.Len() < 1 {
			return e, nil
		}
		if e.TransactionLength, err = buf.LengthEncodedUint64(); err != nil {
			return nil, err
		}

		// Fetch the original/immediate_server_version.
		// If no version can be fetched, set it to UNDEFINED_SERVER_VERSION.
		e.OriginalServerVersion = UndefinedServerVersion
		e.ImmediateServerVersion = UndefinedServerVersion
		if buf.Len() < 4 {
			return e, nil
		}
		if e.ImmediateServerVersion, err = buf.Uint32(); err != nil {
			return nil, err
		}
		if e.ImmediateServerVersion&uint32(1<<31) != 0 {
			e.ImmediateServerVersion &^= 1 << 31
			if e.OriginalServerVersion, err = buf.Uint32(); err != nil {
				return nil, err
			}
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
