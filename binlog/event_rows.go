package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/packet"
	"math"
	"strings"
	"time"
)

type TableMapEvent struct {
	EventHeader
	TableId          uint64
	Flags            TableMapFlag
	Database         string
	Table            string
	Columns          []Column
	OptionalMetadata []byte
}

type TableMapFlag uint16

const (
	TableMapFlagNoFlags     TableMapFlag = 0
	TableMapFlagBitLenExact TableMapFlag = 1 << (iota - 1)
	TableMapFlagReferredFKDB
)

type Column struct {
	BinlogType   packet.TableColumnType
	RealType     packet.TableColumnType
	Nullable     bool
	Meta         uint64
	IsArray      bool
	Unsigned     bool
	Charset      *charset.Charset
	GeometryType GeometryType

	//
	// when SET binlog-row-metadata=FULL
	//
	Name             string
	EnumSetValues    []string
	IsPrimaryKey     bool
	PrimaryKeyPrefix uint64
	IsInvisible      bool
}

type GeometryType uint8

const (
	Geometry GeometryType = iota
	Point
	Linestring
	Polygon
	MultiPoint
	MultiLinestring
	MultiPolygon
	GeometryCollection
)

func (c *Column) HasSignedness() bool {
	return hasSignednessType(c.RealType)
}

func (c *Column) HasCharset() bool {
	return isCharacterType(c.RealType)
}

func (c *Column) HasGeometryType() bool {
	return c.RealType == packet.MySQLTypeGeometry
}

type OptionalMetadataFieldType uint8

const (
	OptionalMetadataFieldTypeSignedness OptionalMetadataFieldType = iota + 1
	OptionalMetadataFieldTypeDefaultCharset
	OptionalMetadataFieldTypeColumnCharset
	OptionalMetadataFieldTypeColumnName
	OptionalMetadataFieldTypeSetStrValue
	OptionalMetadataFieldTypeEnumStrValue
	OptionalMetadataFieldTypeGeometryType
	OptionalMetadataFieldTypePrimaryKey
	OptionalMetadataFieldTypePrimaryKeyWithPrefix
	OptionalMetadataFieldTypeEnumAndSetDefaultCharset
	OptionalMetadataFieldTypeEnumAndSetColumnCharset
	OptionalMetadataFieldTypeColumnVisibility
)

func ParseTableMapEvent(data []byte, fde *FormatDescriptionEvent) (*TableMapEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(TableMapEvent)

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse table id.
	postHeaderLen, ok := fde.PostHeaderLenMap[EventTypeTableMap]
	if !ok {
		return nil, fmt.Errorf("FormatDescription event does not conntain post header length for TableMap event")
	}
	if postHeaderLen == 6 {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(4))
	} else {
		e.TableId = packet.FixedLengthInteger.Uint64(buf.Next(6))
	}

	// Parse flags.
	e.Flags = TableMapFlag(packet.FixedLengthInteger.Uint16(buf.Next(2)))

	// Parse database.
	databaseLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.Database = string(buf.Next(int(databaseLen)))

	buf.Next(1)

	// Parse table.
	tableLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	e.Table = string(buf.Next(int(tableLen)))

	buf.Next(1)

	// Parse column count.
	columnCnt, err := packet.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, err
	}
	e.Columns = make([]Column, columnCnt)

	// Parse column binlog type.
	columnTypes := buf.Next(len(e.Columns))
	for i, columnType := range columnTypes {
		e.Columns[i].BinlogType = packet.TableColumnType(columnType)
	}

	if buf.Len() > 0 {
		// Parse column metadata.
		if err := e.parseColumnMetadata(buf); err != nil {
			return nil, err
		}

		// Parse column real type.
		e.parseRealType()

		// Parse whether column is nullable.
		if err := e.parseNullable(buf); err != nil {
			return nil, err
		}

	}

	// Parse optional metadata.
	if err := e.parseOptionalMetadata(buf); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *TableMapEvent) parseColumnMetadata(buf *bytes.Buffer) error {
	metaData, err := packet.LengthEncodedString.Get(buf)
	if err != nil {
		return err
	}
	pos := 0

	for i := range e.Columns {
		column := &e.Columns[i]

		if column.BinlogType == packet.MySQLTypeTypedArray {
			column.IsArray = true
			column.BinlogType = packet.TableColumnType(metaData[pos])
			pos++
		}

		switch column.BinlogType {
		case packet.MySQLTypeFloat,
			packet.MySQLTypeDouble,
			packet.MySQLTypeTime2,
			packet.MySQLTypeTimestamp2,
			packet.MySQLTypeDatetime2,
			packet.MySQLTypeBlob,
			packet.MySQLTypeGeometry,
			packet.MysSQLTypeJson:

			// These types store a single byte.
			column.Meta = uint64(metaData[pos])
			pos++

		case packet.MySQLTypeString:
			meta := uint64(metaData[pos]) << 8
			meta += uint64(metaData[pos+1])
			pos += 2
			column.Meta = meta

		case packet.MySQLTypeBit:
			meta := uint64(metaData[pos])
			meta += uint64(metaData[pos+1]) << 8
			pos += 2
			column.Meta = meta

		case packet.MySQLTypeVarchar:
			if column.IsArray {
				column.Meta = packet.FixedLengthInteger.Uint64(metaData[pos : pos+3])
				pos += 3
			} else {
				column.Meta = packet.FixedLengthInteger.Uint64(metaData[pos : pos+2])
				pos += 2
			}

		case packet.MySQLTypeNewDecimal:
			meta := uint64(metaData[pos]) << 8
			meta += uint64(metaData[pos+1])
			pos += 2
			column.Meta = meta

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

			// These types have no meta.

		case packet.MySQLTypeEnum,
			packet.MySQLTypeSet,
			packet.MySQLTypeTinyBlob,
			packet.MySQLTypeMediumBlob,
			packet.MySQLTypeLongBlob,
			packet.MySQLTypeVarString:

			// These types are not among binlog types.
			return fmt.Errorf("unexpected column binlog type: %s", column.BinlogType)
		}
	}

	return nil
}

func (e *TableMapEvent) parseRealType() {
	for i := range e.Columns {
		column := &e.Columns[i]

		switch column.BinlogType {
		case packet.MySQLTypeString:
			column.RealType = column.BinlogType
			if t := packet.TableColumnType(column.Meta >> 8); isEnumSetType(t) {
				column.RealType = t
			}
		default:
			column.RealType = column.BinlogType
		}
	}
}

func (e *TableMapEvent) parseNullable(buf *bytes.Buffer) error {
	nullBits, err := createBitmap(len(e.Columns), buf)
	if err != nil {
		return err
	}

	for i := 0; i < len(e.Columns); i++ {
		if nullBits.Get(i) {
			e.Columns[i].Nullable = true
		}
	}

	return nil
}

func (e *TableMapEvent) parseOptionalMetadata(buf *bytes.Buffer) error {
	for buf.Len() > 0 {
		b, err := buf.ReadByte()
		if err != nil {
			return err
		}

		val, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		length := int(val)

		switch OptionalMetadataFieldType(b) {
		case OptionalMetadataFieldTypeSignedness:
			err = e.parseSignedness(buf, length)
		case OptionalMetadataFieldTypeDefaultCharset:
			err = e.parseDefaultCharset(buf, length, false)
		case OptionalMetadataFieldTypeColumnCharset:
			err = e.parseColumnCharset(buf, length, false)
		case OptionalMetadataFieldTypeColumnName:
			err = e.parseColumnName(buf, length)
		case OptionalMetadataFieldTypeSetStrValue:
			err = e.parseEnumSetStrValue(buf, length, false)
		case OptionalMetadataFieldTypeEnumStrValue:
			err = e.parseEnumSetStrValue(buf, length, true)
		case OptionalMetadataFieldTypeGeometryType:
			err = e.parseGeometryType(buf, length)
		case OptionalMetadataFieldTypePrimaryKey:
			err = e.parseSimplePrimaryKey(buf, length)
		case OptionalMetadataFieldTypePrimaryKeyWithPrefix:
			err = e.parsePrimaryKeyWithPrefix(buf, length)
		case OptionalMetadataFieldTypeEnumAndSetDefaultCharset:
			err = e.parseDefaultCharset(buf, length, true)
		case OptionalMetadataFieldTypeEnumAndSetColumnCharset:
			err = e.parseColumnCharset(buf, length, true)
		case OptionalMetadataFieldTypeColumnVisibility:
			err = e.parseColumnVisibility(buf, length)
		default:
			return fmt.Errorf("unsupported optional metadata field type: %d", b)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *TableMapEvent) parseSignedness(buf *bytes.Buffer, length int) error {
	signedness := make([]bool, 0)
	for i := 0; i < length; i++ {
		field, err := buf.ReadByte()
		if err != nil {
			return err
		}

		for c := uint8(0x80); c != 0; c >>= 1 {
			signedness = append(signedness, field&c != 0)
		}
	}

	index := 0
	for i, column := range e.Columns {
		if index >= len(signedness) {
			return ErrInvalidData
		}

		if hasSignednessType(column.RealType) {
			e.Columns[i].Unsigned = signedness[index]
			index++
		}
	}

	return nil
}

func (e *TableMapEvent) parseDefaultCharset(buf *bytes.Buffer, length int, isEnumSet bool) error {
	l := buf.Len()

	defaultCollationId, err := packet.LengthEncodedInteger.Get(buf)
	if err != nil {
		return err
	}
	defaultCollation, err := charset.GetCollation(defaultCollationId)
	if err != nil {
		return err
	}

	type columnPair struct {
		ci uint64
		cs *charset.Charset
	}

	columnPairs := make([]columnPair, 0)
	for l-buf.Len() < length {
		columnIndex, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		columnCollationId, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		collation, err := charset.GetCollation(columnCollationId)
		if err != nil {
			return err
		}
		columnPairs = append(columnPairs, columnPair{
			ci: columnIndex,
			cs: collation.Charset(),
		})
	}

	index := 0
	for i, column := range e.Columns {
		if !isEnumSet && isCharacterType(column.RealType) || isEnumSet && isEnumSetType(column.RealType) {
			if index < len(columnPairs) && columnPairs[index].ci == uint64(i) {
				e.Columns[i].Charset = columnPairs[index].cs
				index++
			} else {
				e.Columns[i].Charset = defaultCollation.Charset()
			}
		}
	}

	return nil
}

func (e *TableMapEvent) parseColumnCharset(buf *bytes.Buffer, length int, isEnumSet bool) error {
	l := buf.Len()

	charsets := make([]*charset.Charset, 0)
	for l-buf.Len() < length {
		collationId, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		collation, err := charset.GetCollation(collationId)
		if err != nil {
			return err
		}
		charsets = append(charsets, collation.Charset())
	}

	index := 0
	for i, column := range e.Columns {
		if index >= len(charsets) {
			return ErrInvalidData
		}

		if !isEnumSet && isCharacterType(column.RealType) || isEnumSet && isEnumSetType(column.RealType) {
			e.Columns[i].Charset = charsets[index]
			index++
		}
	}

	return nil
}

func (e *TableMapEvent) parseColumnName(buf *bytes.Buffer, length int) error {
	l := buf.Len()

	index := 0
	for l-buf.Len() < length {
		if index >= len(e.Columns) {
			return ErrInvalidData
		}

		name, err := packet.LengthEncodedString.Get(buf)
		if err != nil {
			return err
		}

		e.Columns[index].Name = string(name)
		index++
	}

	return nil
}

func (e *TableMapEvent) parseEnumSetStrValue(buf *bytes.Buffer, length int, isEnum bool) error {
	l := buf.Len()

	columnValues := make([][]string, 0)
	for l-buf.Len() < length {
		count, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}

		values := make([]string, count)
		for i := uint64(0); i < count; i++ {
			val, err := packet.LengthEncodedString.Get(buf)
			if err != nil {
				return nil
			}
			values = append(values, string(val))
		}

		columnValues = append(columnValues, values)
	}

	index := 0
	for i, column := range e.Columns {
		if index >= len(columnValues) {
			return ErrInvalidData
		}

		if isEnum && column.RealType == packet.MySQLTypeEnum || !isEnum && column.RealType == packet.MySQLTypeSet {
			e.Columns[i].EnumSetValues = columnValues[index]
			index++
		}
	}

	return nil
}

func (e *TableMapEvent) parseGeometryType(buf *bytes.Buffer, length int) error {
	l := buf.Len()

	types := make([]int, 0)
	for l-buf.Len() < length {
		t, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		types = append(types, int(t))
	}

	index := 0
	for i, column := range e.Columns {
		if index >= len(types) {
			return ErrInvalidData
		}

		if column.RealType == packet.MySQLTypeGeometry {
			e.Columns[i].GeometryType = GeometryType(types[index])
			index++
		}
	}

	return nil
}

func (e *TableMapEvent) parseSimplePrimaryKey(buf *bytes.Buffer, length int) error {
	l := buf.Len()

	for l-buf.Len() < length {
		columnIndex, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}

		if int(columnIndex) >= len(e.Columns) {
			return ErrInvalidData
		}
		e.Columns[int(columnIndex)].IsPrimaryKey = true
	}

	return nil
}

func (e *TableMapEvent) parsePrimaryKeyWithPrefix(buf *bytes.Buffer, length int) error {
	l := buf.Len()

	for l-buf.Len() < length {
		columnIndex, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}
		primaryKeyPrefix, err := packet.LengthEncodedInteger.Get(buf)
		if err != nil {
			return err
		}

		if int(columnIndex) >= len(e.Columns) {
			return ErrInvalidData
		}

		e.Columns[int(columnIndex)].IsPrimaryKey = true
		e.Columns[int(columnIndex)].PrimaryKeyPrefix = primaryKeyPrefix
	}

	return nil
}

func (e *TableMapEvent) parseColumnVisibility(buf *bytes.Buffer, length int) error {
	visibility := make([]bool, 0)
	for i := 0; i < length; i++ {
		field, err := buf.ReadByte()
		if err != nil {
			return err
		}

		for c := uint8(0x80); c != 0; c >>= 1 {
			visibility = append(visibility, field&c != 0)
		}
	}

	index := 0
	for i := range e.Columns {
		if index >= len(visibility) {
			return ErrInvalidData
		}

		if !visibility[index] {
			e.Columns[i].IsInvisible = true
			index++
		}
	}

	return nil
}

func (e *TableMapEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Table id: %d\n", e.TableId)
	fmt.Fprintf(sb, "Flags: %d\n", e.Flags)
	fmt.Fprintf(sb, "Database: %s\n", e.Database)
	fmt.Fprintf(sb, "Table: %s\n", e.Table)
	fmt.Fprintf(sb, "Column count: %d\n", len(e.Columns))

	columns := make([]string, len(e.Columns))
	for i, column := range e.Columns {
		columnInfo := []string{
			fmt.Sprintf("binlog_type(%s)", column.BinlogType),
			fmt.Sprintf("real_type(%s)", column.RealType),
			fmt.Sprintf("null(%t)", column.Nullable),
			fmt.Sprintf("array(%t)", column.IsArray),
			fmt.Sprintf("meta(%d)", column.Meta),
		}
		if column.HasSignedness() {
			columnInfo = append(columnInfo, fmt.Sprintf("unsigned(%t)", column.Unsigned))
		}
		if column.HasCharset() {
			columnInfo = append(columnInfo, fmt.Sprintf("charset(%s)", column.Charset.Name()))
		}
		columns[i] = strings.Join(columnInfo, " ")
	}
	fmt.Fprintf(sb, "Column info: [\n\t%s\n]\n", strings.Join(columns, "\n\t"))

	return sb.String()
}

type RowsEvent struct {
	// Parse context.
	Table    *TableMapEvent
	Location *time.Location

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

	ColumnCnt          uint64
	ColumnsBeforeImage *BitSet
	ColumnsAfterImage  *BitSet

	Rows []Row
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

type Row []ColumnValue

type ColumnValue struct {
	ColumnIndex int
	IsNull      bool
	Value       interface{}
}

func ParseRowsEvent(data []byte, fde *FormatDescriptionEvent, table *TableMapEvent) (*RowsEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(RowsEvent)
	e.Table = table

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse table id.
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

	// Parse flags.
	e.Flags = RowsFlag(packet.FixedLengthInteger.Uint16(buf.Next(2)))

	// Parse extra data.
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

	// Parse column count.
	columnCnt, err := packet.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, err
	}
	e.ColumnCnt = columnCnt

	// Parse column before image.
	if e.ColumnsBeforeImage, err = createBitmap(int(e.ColumnCnt), buf); err != nil {
		return nil, err
	}

	// Parse column after image.
	if eventType == EventTypeUpdateRowsV2 ||
		eventType == EventTypeUpdateRowsV1 ||
		eventType == EventTypePartialUpdateRows {
		if e.ColumnsAfterImage, err = createBitmap(int(e.ColumnCnt), buf); err != nil {
			return nil, err
		}
	} else {
		e.ColumnsAfterImage = e.ColumnsBeforeImage
	}

	// TODO row
	// TODO json partial
	for buf.Len() > 0 {
		switch eventType {
		case EventTypeWriteRowsV2:
			nullBits, err := createBitmap(e.ColumnsAfterImage.Count(), buf)
			if err != nil {
				return nil, err
			}

		case EventTypeDeleteRowsV2:

		case EventTypeUpdateRowsV2:

		}
	}

	return e, nil
}

func (e *RowsEvent) parseRow(buf *bytes.Buffer, isUpdateAfter bool) error {
	columnBits := e.ColumnsBeforeImage
	if isUpdateAfter {
		columnBits = e.ColumnsAfterImage
	}

	// TODO PARTIAL_JSON_UPDATES

	nullBits, err := createBitmap(columnBits.Count(), buf)
	if err != nil {
		return err
	}

	var row Row

	index := 0
	for i := 0; i < int(e.ColumnCnt); i++ {
		cv := ColumnValue{ColumnIndex: i}

		if !columnBits.Get(i) {
			continue
		}

		isNull := nullBits.Get(index)
		index++

		if isNull {
			cv.IsNull = true
		}

		row = append(row, cv)
	}
}

func (e *RowsEvent) parseColumnValue(buf *bytes.Buffer, index int) (interface{}, error) {
	column := e.Table.Columns[index]

	realType := column.RealType
	meta := column.Meta

	var unsigned bool
	if column.HasSignedness() {
		unsigned = column.Unsigned
	}

	var length int
	if realType == packet.MySQLTypeVarString {
		if meta >= 256 {
			byte0 := uint8(meta >> 8)
			byte1 := uint8(meta & 0xff)

			if byte0&0x30 != 0x30 {
				length = int(byte1) | int((byte0&0x30)^0x30)<<4
				// realType = byte0 | 0x30
			} else {
				length = int(byte1)
				// realType = byte0
			}
		} else {
			length = int(meta)
		}
	}

	switch realType {
	case packet.MySQLTypeLong:
		val := packet.FixedLengthInteger.Uint32(buf.Next(4))
		if unsigned {
			return val, nil
		}
		return int32(val), nil

	case packet.MySQLTypeTiny:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return b, nil
		}
		return int8(b), nil

	case packet.MySQLTypeShort:
		val := packet.FixedLengthInteger.Uint16(buf.Next(2))
		if unsigned {
			return val, nil
		}
		return int16(val), nil

	case packet.MySQLTypeInt24:
		val := packet.FixedLengthInteger.Uint32(buf.Next(3))
		if unsigned {
			return val, nil
		}
		return int32(val), nil

	case packet.MySQLTypeLongLong:
		val := packet.FixedLengthInteger.Uint64(buf.Next(8))
		if unsigned {
			return val, nil
		}
		return int64(val), nil

	case packet.MySQLTypeNewDecimal:
		// TODO parse decimal
		//precision := int(meta >> 8)
		//decimals := int(meta & 0xFF)
		return nil, fmt.Errorf("unsuuport new decimal type")

	case packet.MySQLTypeFloat:
		return math.Float32frombits(binary.LittleEndian.Uint32(buf.Next(4))), nil

	case packet.MySQLTypeDouble:
		return math.Float64frombits(binary.LittleEndian.Uint64(buf.Next(8))), nil

	case packet.MySQLTypeBit:
		// TODO convert to int?
		bitNum := ((meta >> 8) * 8) + (meta & 0xFF)
		length = int((bitNum + 7) / 8)
		return buf.Next(length), nil

	case packet.MySQLTypeTimestamp:
		sec := binary.LittleEndian.Uint32(buf.Next(4))
		if sec == 0 {
			return time.Time{}, nil
		}

		t := time.Unix(int64(sec), 0)
		if e.Location != nil {
			return t.In(e.Location), nil
		}
		return t, nil

	case packet.MySQLTypeTimestamp2:
		sec := binary.BigEndian.Uint32(buf.Next(4))

		var microSec int64
		switch meta {
		case 0:
			microSec = 0
		case 1, 2:
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			microSec = int64(b) * 10000
		case 3, 4:
			microSec = int64(binary.BigEndian.Uint16(buf.Next(2))) * 100
		case 5, 6:
			b := buf.Next(3)
			microSec = int64(uint32(b[2]) | uint32(b[1])<<8 | uint32(b[0])<<16)
		default:
			return nil, fmt.Errorf("invalid meta %d for %s", meta, packet.MySQLTypeTimestamp2)
		}

		if sec == 0 && microSec == 0 {
			return time.Time{}, nil
		}

		t := time.Unix(int64(sec), microSec*1000)
		if e.Location != nil {
			return t.In(e.Location), nil
		}
		return t, nil

	case packet.MySQLTypeDatetime:
		i64 := binary.LittleEndian.Uint64(buf.Next(8))
		d := i64 / 1000000
		t := i64 % 1000000

		if i64 == 0 {
			return time.Time{}, nil
		}

		loc := time.Local
		if e.Location != nil {
			loc = e.Location
		}

		return time.Date(int(d/10000), time.Month((d%10000)/100), int(d%100),
			int(t/10000), int((t%10000)/100), int(t%100), 0, loc), nil

	case packet.MySQLTypeDatetime2:
		b := buf.Next(5)
		intPart := int64(uint64(b[4])|uint64(b[3])<<8|uint64(b[2])<<16|uint64(b[1])<<24|uint64(b[0]))<<32 - 0x8000000000

		var frac int64
		switch meta {
		case 0:
			frac = 0
		case 1, 2:
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			frac = int64(b) * 10000
		case 3, 4:
			frac = int64(binary.BigEndian.Uint16(buf.Next(2))) * 100
		case 5, 6:
			b := buf.Next(3)
			frac = int64(uint32(b[2]) | uint32(b[1])<<8 | uint32(b[0])<<16)
		default:
			return nil, fmt.Errorf("invalid meta %d for %s", meta, packet.MySQLTypeDatetime2)
		}

		if intPart == 0 && frac == 0 {
			return time.Time{}, nil
		}

		ymd := intPart >> 17
		ym := ymd >> 5
		hms := intPart % (1 << 17)

		day := ymd % (1 << 5)
		month := ym % 13
		year := ym / 13

		second := hms % (1 << 6)
		minute := (hms >> 6) % (1 << 6)
		hour := hms >> 12

		// TODO return string if before 1970-01-01 00:00:00

		loc := time.Local
		if e.Location != nil {
			loc = e.Location
		}

		return time.Date(int(year), time.Month(month), int(day),
			int(hour), int(minute), int(second), int(frac*1000), loc), nil

	case packet.MySQLTypeTime:

	}

}
