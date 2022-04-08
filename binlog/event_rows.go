package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet"
	"strings"
)

type TableMapEvent struct {
	EventHeader
	TableId  uint64
	Flags    TableMapFlag
	Database string
	Table    string
	//ColumnCount uint64
	//ColumnTypes []packet.TableColumnType
	//ColumnMetas []ColumnMeta
	//NullBitmap  []byte
	Column []Column

	OptionalMetadata []byte
}

type TableMapFlag uint16

const (
	TableMapFlagNoFlags     TableMapFlag = 0
	TableMapFlagBitLenExact TableMapFlag = 1 << iota
	TableMapFlagReferredFKDB
)

type Column struct {
	Type packet.TableColumnType
	Meta uint64
}

func ParseTableMapEvent(data []byte, fde *FormatDescriptionEvent) (*TableMapEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(TableMapEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Table id
	postHeaderLen, ok := fde.PostHeaderLenMap[EventTypeTableMap]
	if !ok {
		return nil, fmt.Errorf("FormatDescription event does not conntain post header length for TableMap event")
	}
	if postHeaderLen == 6 {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(4))
	} else {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(6))
	}

	// Flags
	e.Flags = TableMapFlag(packet.FixedLengthInteger.Uint16(buf.Next(2)))

	// Database
	databaseLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.Database = string(buf.Next(int(databaseLen)))

	buf.Next(1)

	// Table
	tableLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.Table = string(buf.Next(int(tableLen)))

	buf.Next(1)

	// Column count
	columnCount, err := packet.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, err
	}

	// Column type definition
	columnTypes := buf.Next(int(e.ColumnCount))
	e.ColumnTypes = make([]packet.TableColumnType, len(columnTypes))
	for i, columnType := range columnTypes {
		e.ColumnTypes[i] = packet.TableColumnType(columnType)
	}

	if buf.Len() > 0 {
		metaData, err := packet.LengthEncodedString.Get(buf)
		if err != nil {
			return nil, err
		}
		pos := 0

		e.ColumnMetas = make([]ColumnMeta, e.ColumnCount)
		for i, columnType := range e.ColumnTypes {
			// TODO Field_typed_array
			switch columnType {
			case packet.MySQLTypeNewDecimal:
				var m NewDecimalMeta
				m.Precision = metaData[pos]
				pos++
				m.Decimals = metaData[pos]
				pos++
				e.ColumnMetas[i] = m
			case packet.MySQLTypeFloat:
				e.ColumnMetas[i] = FloatMeta{Len: metaData[pos]}
				pos++
			case packet.MySQLTypeDouble:
				e.ColumnMetas[i] = DoubleMeta{Len: metaData[pos]}
				pos++
			case packet.MySQLTypeString:
				var m StringMeta
				// Real type (upper) and length (lower) values
				paramData := binary.BigEndian.Uint16(metaData[pos : pos+2])
				pos += 2
				// TODO paramData == 0?  mysql source
				m.RealType = packet.TableColumnType(paramData >> 8)
				fromLen := ((paramData >> 4) & 0x300) ^ 0x300 + paramData&0x00ff
				if fromLen > 255 {
					m.LenByteNum = 2
				} else {
					m.LenByteNum = 1
				}
				e.ColumnMetas[i] = m
			case packet.MySQLTypeVarchar:
				var m VarStringMeta
				paramData := binary.LittleEndian.Uint16(metaData[pos : pos+2])
				pos += 2
				// TODO paramData == 0?  mysql source
				if paramData <= 255 {
					m.LenByteNum = 1
				} else {
					m.LenByteNum = 2
				}
				e.ColumnMetas[i] = m
			case packet.MySQLTypeBlob, packet.MySQLTypeGeometry, packet.MysSQLTypeJson:
				var m BlobMeta
				m.LenByteNum = metaData[pos]
				pos++
				e.ColumnMetas[i] = m
			//case packet.MySQLTypeEnum, packet.MySQLTypeSet:
			//	var m EnumMeta
			//	m.RealType = packet.TableColumnType(metaData[pos])
			//	pos++
			//	m.Len = metaData[pos]
			//	pos++
			//	e.ColumnMetas[i] = m
			case packet.MySQLTypeBit:
				var m BitMeta
				paramData := binary.BigEndian.Uint16(metaData[pos : pos+2])
				pos += 2
				m.BitLen = uint8(paramData >> 8)
				m.ByteNum = uint8(paramData)
				e.ColumnMetas[i] = m
			case packet.MySQLTypeTimestamp2, packet.MySQLTypeDatetime2:
				var m DateTimeF
				m.Decimals = metaData[pos]
				pos++
				e.ColumnMetas[i] = m
			case packet.MySQLTypeTime2:
				var m TimeF
				m.Decimals = metaData[pos]
				pos++
				e.ColumnMetas[i] = m
			case packet.MySQLTypeDecimal,
				packet.MySQLTypeTiny,
				packet.MySQLTypeShort,
				packet.MySQLTypeInt24,
				packet.MySQLTypeLong,
				packet.MySQLTypeLongLong,
				packet.MySQLTypeTimestamp,
				packet.MySQLTypeYear,
				packet.MySQLTypeDate,
				packet.MySQLTypeTime,
				packet.MySQLTypeDatetime:
				e.ColumnMetas[i] = EmptyColumnMeta{}
			case packet.MySQLTypeEnum,
				packet.MySQLTypeSet,
				packet.MySQLTypeTinyBlob,
				packet.MySQLTypeMediumBlob,
				packet.MySQLTypeLongBlob,
				packet.MySQLTypeVarString:
				return nil, fmt.Errorf("unexpected field type: %s", columnType)
			}
		}

		e.NullBitmap = buf.Next(int((e.ColumnCount + 7) / 8))
	}

	if buf.Len() > 0 {
		e.OptionalMetadata = buf.Bytes()
	}

	return e, nil
}

func (e *TableMapEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Table id: %d\n", e.TableId)
	fmt.Fprintf(sb, "Flags: %d\n", e.Flags)
	fmt.Fprintf(sb, "Database: %s\n", e.Database)
	fmt.Fprintf(sb, "Table: %s\n", e.Table)
	fmt.Fprintf(sb, "Column count: %d\n", e.ColumnCount)

	columnTypes := make([]string, e.ColumnCount)
	for i, columnType := range e.ColumnTypes {
		columnTypes[i] = columnType.String()
	}
	fmt.Fprintf(sb, "Column types: %s\n", strings.Join(columnTypes, ", "))

	// TODO

	return sb.String()
}

type RowsEvent struct {
	EventHeader
	TableId uint64
	Flags   RowsFlag

	// TODO parse
	ExtraRowNDBInfo []byte

	// For a row in a partitioned table.
	ExtraPartitionId uint16

	// It is the partition_id of the source partition in case
	// of Update Event, the target's partition id is PartitionId.
	// This variable is used only in case of Update Event.
	ExtraSourcePartitionId uint16

	ColumnCount        uint64
	ColumnsBeforeImage *BitSet
	ColumnsAfterImage  *BitSet
}

type RowsFlag uint16

const (
	// RowsFlagStmtEnd indicates the last event of a statement.
	RowsFlagStmtEnd RowsFlag = 1 << iota

	// RowsFlagNoForeignKeyChecks indicates no foreign key checks.
	RowsFlagNoForeignKeyChecks

	// RowsFlagRelaxedUniqueChecks indicates no unique key checks.
	RowsFlagRelaxedUniqueChecks

	// RowsFlagCompleteRows indicates that rows in this event are complete, that is contain
	// values for all columns of the table.
	RowsFlagCompleteRows
)

type ExtraRowInfoTypeCode uint8

const (
	ExtraRowInfoTypeCodeNDB ExtraRowInfoTypeCode = iota
	ExtraRowInfoTypeCodePART
)

func ParseRowsEvent(data []byte, fde *FormatDescriptionEvent) (*RowsEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(RowsEvent)

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Table id
	eventType := e.EventHeader.EventType
	postHeaderLen, ok := fde.PostHeaderLenMap[eventType]
	if !ok {
		return nil, fmt.Errorf("FormatDescription event does not conntain post header length for %s", eventType)
	}
	if postHeaderLen == 6 {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(4))
	} else {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(6))
	}

	// Flags
	e.Flags = RowsFlag(packet.FixedLengthInteger.Uint16(buf.Next(2)))

	// Extra data
	if postHeaderLen == 10 {
		headerLen := packet.FixedLengthInteger.Uint16(buf.Next(2))
		headerLen -= 2

		before := buf.Len()
		for before-buf.Len() < int(headerLen) {
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			switch ExtraRowInfoTypeCode(b) {
			case ExtraRowInfoTypeCodeNDB:
				b, err = buf.ReadByte()
				if err != nil {
					return nil, err
				}
				info := buf.Next(int(b) - 1)
				// NDB info len is part of the buffer to be copied below.
				e.ExtraRowNDBInfo = append([]byte{b}, info...)
			case ExtraRowInfoTypeCodePART:
				e.ExtraPartitionId = packet.FixedLengthInteger.Uint16(buf.Next(2))
				if eventType == EventTypeUpdateRowsV2 ||
					eventType == EventTypeUpdateRowsV1 ||
					eventType == EventTypePartialUpdateRows {
					e.ExtraSourcePartitionId = packet.FixedLengthInteger.Uint16(buf.Next(2))
				}
			default:
				return nil, fmt.Errorf("unsupported extra row info type")
			}
		}
	}

	// Column count
	columnCount, err := packet.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, err
	}
	e.ColumnCount = columnCount

	if e.ColumnsBeforeImage, err = createBitmap(int(e.ColumnCount), buf); err != nil {
		return nil, err
	}

	if eventType == EventTypeUpdateRowsV2 ||
		eventType == EventTypeUpdateRowsV1 ||
		eventType == EventTypePartialUpdateRows {
		if e.ColumnsAfterImage, err = createBitmap(int(e.ColumnCount), buf); err != nil {
			return nil, err
		}
	} else {
		e.ColumnsAfterImage = e.ColumnsBeforeImage
	}

	// TODO row

	return e, nil
}

func ParseRowsEventRows() {

}

//type WriteRowsEvent struct {
//	EventHeader
//}
//
//func ParseWriteRowsEvent(data []byte) (*WriteRowsEvent, error) {
//	buf := bytes.NewBuffer(data)
//	e := new(WriteRowsEvent)
//
//	// Event header
//	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
//		return nil, err
//	}
//
//	// TODO
//
//	return e, nil
//}

type ColumnMeta interface {
	//fmt.Stringer
	IsEmpty() bool
}

type EmptyColumnMeta struct{}

func (e EmptyColumnMeta) IsEmpty() bool {
	return true
}

type NotEmptyColumnMeta struct{}

func (e NotEmptyColumnMeta) IsEmpty() bool {
	return false
}

type NewDecimalMeta struct {
	NotEmptyColumnMeta
	Precision uint8
	Decimals  uint8
}

type FloatMeta struct {
	NotEmptyColumnMeta
	Len uint8
}

type DoubleMeta struct {
	NotEmptyColumnMeta
	Len uint8
}

type StringMeta struct {
	NotEmptyColumnMeta
	RealType   packet.TableColumnType
	LenByteNum uint8
}

type VarStringMeta struct {
	NotEmptyColumnMeta
	LenByteNum uint8
}

type BlobMeta struct {
	NotEmptyColumnMeta
	LenByteNum uint8
}

//type EnumMeta struct {
//	NotEmptyColumnMeta
//	RealType packet.TableColumnType
//	Len      uint8
//}

type BitMeta struct {
	NotEmptyColumnMeta
	BitLen  uint8
	ByteNum uint8
}

type DateTimeF struct {
	NotEmptyColumnMeta
	Decimals uint8
}

type TimeF struct {
	NotEmptyColumnMeta
	Decimals uint8
}
