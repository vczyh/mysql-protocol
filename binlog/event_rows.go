package binlog

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/mysql"
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
	Index int

	BinlogType   packet.TableColumnType
	RealType     packet.TableColumnType
	Nullable     bool
	Meta         uint64
	IsArray      bool
	Unsigned     bool
	Charset      *charset.Charset
	GeometryType GeometryType

	//
	// when binlog-row-metadata=FULL
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

func ParseTableMapEvent(data []byte, fde *FormatDescriptionEvent) (e *TableMapEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(TableMapEvent)

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
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		e.TableId = uint64(u)
	} else {
		if e.TableId, err = buf.Uint48(); err != nil {
			return nil, err
		}
	}

	// Parse flags.
	u, err := buf.Uint16()
	if err != nil {
		return nil, err
	}
	e.Flags = TableMapFlag(u)

	// Parse database.
	databaseLen, err := buf.Uint8()
	if err != nil {
		return nil, err
	}
	if e.Database, err = buf.NextString(int(databaseLen)); err != nil {
		return nil, err
	}

	_, _ = buf.Next(1)

	// Parse table.
	tableLen, err := buf.Uint8()
	if err != nil {
		return nil, err
	}
	if e.Table, err = buf.NextString(int(tableLen)); err != nil {
		return nil, err
	}

	_, _ = buf.Next(1)

	// Parse column count.
	columnCnt, err := buf.LengthEncodedUint64()
	if err != nil {
		return nil, err
	}
	e.Columns = make([]Column, columnCnt)

	// Parse column binlog type.
	columnTypes, err := buf.Next(len(e.Columns))
	if err != nil {
		return nil, err
	}
	for i, columnType := range columnTypes {
		e.Columns[i].Index = i
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

func (e *TableMapEvent) parseColumnMetadata(buf *mysql.Buffer) error {
	metaData, err := buf.LengthEncodedBytes()
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
				// TODO replace method
				column.Meta = packet.FixedLengthInteger.Uint64(metaData[pos : pos+3])
				pos += 3
			} else {
				// TODO replace method
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

func (e *TableMapEvent) parseNullable(buf *mysql.Buffer) error {
	nullBits, err := buf.CreateBitmap(len(e.Columns))
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

func (e *TableMapEvent) parseOptionalMetadata(buf *mysql.Buffer) error {
	for buf.Len() > 0 {
		b, err := buf.ReadByte()
		if err != nil {
			return err
		}

		length, err := buf.LengthEncodedInt()
		if err != nil {
			return err
		}

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

func (e *TableMapEvent) parseSignedness(buf *mysql.Buffer, length int) error {
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

func (e *TableMapEvent) parseDefaultCharset(buf *mysql.Buffer, length int, isEnumSet bool) error {
	l := buf.Len()

	defaultCollationId, err := buf.LengthEncodedUint64()
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
		columnIndex, err := buf.LengthEncodedUint64()
		if err != nil {
			return err
		}

		columnCollationId, err := buf.LengthEncodedUint64()
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

func (e *TableMapEvent) parseColumnCharset(buf *mysql.Buffer, length int, isEnumSet bool) error {
	l := buf.Len()

	charsets := make([]*charset.Charset, 0)
	for l-buf.Len() < length {
		collationId, err := buf.LengthEncodedUint64()
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

func (e *TableMapEvent) parseColumnName(buf *mysql.Buffer, length int) (err error) {
	l := buf.Len()

	index := 0
	for l-buf.Len() < length {
		if index >= len(e.Columns) {
			return ErrInvalidData
		}

		if e.Columns[index].Name, err = buf.LengthEncodedString(); err != nil {
			return err
		}
		index++
	}

	return nil
}

func (e *TableMapEvent) parseEnumSetStrValue(buf *mysql.Buffer, length int, isEnum bool) error {
	l := buf.Len()

	columnValues := make([][]string, 0)
	for l-buf.Len() < length {
		count, err := buf.LengthEncodedInt()
		if err != nil {
			return err
		}

		values := make([]string, count)
		for i := 0; i < count; i++ {
			val, err := buf.LengthEncodedString()
			if err != nil {
				return nil
			}
			values = append(values, val)
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

func (e *TableMapEvent) parseGeometryType(buf *mysql.Buffer, length int) error {
	l := buf.Len()

	types := make([]int, 0)
	for l-buf.Len() < length {
		t, err := buf.LengthEncodedInt()
		if err != nil {
			return err
		}
		types = append(types, t)
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

func (e *TableMapEvent) parseSimplePrimaryKey(buf *mysql.Buffer, length int) error {
	l := buf.Len()

	for l-buf.Len() < length {
		columnIndex, err := buf.LengthEncodedInt()
		if err != nil {
			return err
		}

		if columnIndex >= len(e.Columns) {
			return ErrInvalidData
		}

		e.Columns[columnIndex].IsPrimaryKey = true
	}

	return nil
}

func (e *TableMapEvent) parsePrimaryKeyWithPrefix(buf *mysql.Buffer, length int) error {
	l := buf.Len()

	for l-buf.Len() < length {
		columnIndex, err := buf.LengthEncodedInt()
		if err != nil {
			return err
		}

		primaryKeyPrefix, err := buf.LengthEncodedUint64()
		if err != nil {
			return err
		}

		if columnIndex >= len(e.Columns) {
			return ErrInvalidData
		}

		e.Columns[columnIndex].IsPrimaryKey = true
		e.Columns[columnIndex].PrimaryKeyPrefix = primaryKeyPrefix
	}

	return nil
}

func (e *TableMapEvent) parseColumnVisibility(buf *mysql.Buffer, length int) error {
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
	fmt.Fprintf(sb, "Column info: \n\t%s\n", strings.Join(columns, "\n\t"))

	return sb.String()
}

type RowsEvent struct {
	// Parser context.
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
	ColumnsBeforeImage *mysql.BitSet
	ColumnsAfterImage  *mysql.BitSet

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

func ParseRowsEvent(data []byte, fde *FormatDescriptionEvent, parser *Parser) (e *RowsEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(RowsEvent)

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse table id.
	eventType := e.EventType
	postHeaderLen, ok := fde.PostHeaderLenMap[eventType]
	if !ok {
		return nil, fmt.Errorf("FormatDescription event does not conntain post header length for %s", eventType)
	}
	if postHeaderLen == 6 {
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		e.TableId = uint64(u)
	} else {
		if e.TableId, err = buf.Uint48(); err != nil {
			return nil, err
		}
	}

	table, err := parser.TableMapEvent(e.TableId)
	if err != nil {
		return nil, err
	}
	e.Table = table

	// Parse flags.
	u, err := buf.Uint16()
	if err != nil {
		return nil, err
	}
	e.Flags = RowsFlag(u)

	// Parse extra data.
	if postHeaderLen == 10 {
		headerLen, err := buf.Uint16()
		if err != nil {
			return nil, err
		}
		headerLen -= 2

		before := buf.Len()
		for before-buf.Len() < int(headerLen) {
			b, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}

			switch ExtraRowInfoTypeCode(b) {
			case ExtraRowInfoTypeCodeNDB:
				b, err = buf.Uint8()
				if err != nil {
					return nil, err
				}

				info, err := buf.Next(int(b) - 1)
				if err != nil {
					return nil, err
				}

				// NDB info len is part of the buffer to be copied below.
				e.ExtraRowNDBInfo = append([]byte{b}, info...)

			case ExtraRowInfoTypeCodePART:
				if e.ExtraPartitionId, err = buf.Uint16(); err != nil {
					return nil, err
				}

				if eventType == EventTypeUpdateRowsV2 ||
					eventType == EventTypeUpdateRowsV1 || eventType == EventTypePartialUpdateRows {
					if e.ExtraSourcePartitionId, err = buf.Uint16(); err != nil {
						return nil, err
					}
				}

			default:
				return nil, fmt.Errorf("unsupported extra row info type")
			}
		}
	}

	// Parse column count.
	columnCnt, err := buf.LengthEncodedUint64()
	if err != nil {
		return nil, err
	}

	if int(columnCnt) != len(e.Table.Columns) {
		return nil, ErrInvalidData
	}
	e.ColumnCnt = columnCnt

	// Parse column before image.
	if e.ColumnsBeforeImage, err = buf.CreateBitmap(int(e.ColumnCnt)); err != nil {
		return nil, err
	}

	// Parse column after image.
	if eventType == EventTypeUpdateRowsV2 ||
		eventType == EventTypeUpdateRowsV1 || eventType == EventTypePartialUpdateRows {
		if e.ColumnsAfterImage, err = buf.CreateBitmap(int(e.ColumnCnt)); err != nil {
			return nil, err
		}
	} else {
		e.ColumnsAfterImage = e.ColumnsBeforeImage
	}

	// TODO row
	// TODO json partial
	for buf.Len() > 0 {
		if err := e.parseRow(buf, false); err != nil {
			return nil, err
		}

		if eventType == EventTypeUpdateRowsV2 ||
			eventType == EventTypeUpdateRowsV1 || eventType == EventTypePartialUpdateRows {
			if err := e.parseRow(buf, true); err != nil {
				return nil, err
			}
		}
	}

	return e, nil
}

func (e *RowsEvent) RowColumns(rowindex int) []Column {
	if rowindex >= len(e.Rows) {
		return nil
	}

	row := e.Rows[rowindex]
	columns := make([]Column, len(row))
	for i, val := range row {
		columns[i] = e.Table.Columns[val.ColumnIndex]
	}

	return columns
}

func (e *RowsEvent) RowColumnNames(rowindex int) []string {
	columns := e.RowColumns(rowindex)

	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}

	return names
}

func (e *RowsEvent) RowColumnIndexes(rowindex int) []int {
	columns := e.RowColumns(rowindex)

	indexes := make([]int, len(columns))
	for i, column := range columns {
		indexes[i] = column.Index
	}

	return indexes
}

func (e *RowsEvent) RowValues(rowindex int) []interface{} {
	if rowindex >= len(e.Rows) {
		return nil
	}

	row := e.Rows[rowindex]
	values := make([]interface{}, len(row))
	for i, val := range row {
		if val.IsNull == true {
			values[i] = nil
		} else {
			values[i] = val.Value
		}
	}

	return values
}

func (e *RowsEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Table id: %d\n", e.TableId)
	fmt.Fprintf(sb, "Flags: %d\n", e.Flags)

	// TODO extra data
	fmt.Fprintf(sb, "Column count: %d\n", e.ColumnCnt)

	//var command, clause1, clause2 string
	//switch e.EventType {
	//case EventTypeWriteRowsV2:
	//	command = "INSERT INTO"
	//	clause1 = "SET"
	//case EventTypeDeleteFile:
	//	command = "DELETE FROM"
	//	clause1 = "WHERE"
	//case EventTypeUpdateRowsV2,EventTypePartialUpdateRows:
	//	command = "UPDATE"
	//	clause1 = "WHERE"
	//	clause2 = "SET"
	//}

	var sqls []string
	index := 0
	for index < len(e.Rows) {
		db := e.Table.Database + "." + e.Table.Table

		var clause1Names []string
		for _, v := range e.RowColumnIndexes(index) {
			clause1Names = append(clause1Names, fmt.Sprintf("$%d", v))
		}

		var clause1Values []string
		for _, v := range e.RowValues(index) {
			clause1Values = append(clause1Values, fmt.Sprintf("%+v", v))
		}

		var sql string
		switch e.EventType {
		case EventTypeWriteRowsV2:
			sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				db,
				strings.Join(clause1Names, ", "),
				strings.Join(clause1Values, ", "))
		case EventTypeDeleteFile:
			sql = fmt.Sprintf("DELETE FROM %s WHERE %s",
				db,
				strings.Join(clause1Values, " AND "))
		case EventTypeUpdateRowsV2, EventTypePartialUpdateRows:
			// TODO

		}

		sqls = append(sqls, sql)
		index++
	}

	fmt.Fprintf(sb, "DML: \n\t%s\n", strings.Join(sqls, "\n\t"))

	return sb.String()
}

func (e *RowsEvent) parseRow(buf *mysql.Buffer, isUpdateAfter bool) error {
	columnBits := e.ColumnsBeforeImage
	if isUpdateAfter {
		columnBits = e.ColumnsAfterImage
	}

	// TODO PARTIAL_JSON_UPDATES

	nullBits, err := buf.CreateBitmap(columnBits.Count())
	if err != nil {
		return err
	}

	var row Row
	index := 0
	for i := 0; i < int(e.ColumnCnt); i++ {
		if !columnBits.Get(i) {
			continue
		}

		cv := ColumnValue{ColumnIndex: i}

		isNull := nullBits.Get(index)
		index++

		if isNull {
			cv.IsNull = true
		} else {
			value, err := e.parseColumnValue(buf, i)
			if err != nil {
				return err
			}
			cv.Value = value
		}
		row = append(row, cv)
	}
	e.Rows = append(e.Rows, row)

	return nil
}

func (e *RowsEvent) parseColumnValue(buf *mysql.Buffer, index int) (interface{}, error) {
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
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int32(u), nil

	case packet.MySQLTypeTiny:
		u, err := buf.Uint8()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int8(u), nil

	case packet.MySQLTypeShort:
		u, err := buf.Uint16()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int16(u), nil

	case packet.MySQLTypeInt24:
		u, err := buf.Uint24()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int32(u), nil

	case packet.MySQLTypeLongLong:
		u, err := buf.Uint64()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int64(u), nil

	case packet.MySQLTypeNewDecimal:
		// TODO parse decimal
		//precision := int(meta >> 8)
		//decimals := int(meta & 0xFF)
		return nil, fmt.Errorf("unsuuport new decimal type")

	case packet.MySQLTypeFloat:
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(u), nil

	case packet.MySQLTypeDouble:
		u, err := buf.Uint64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(u), nil

	case packet.MySQLTypeBit:
		// TODO convert to int?
		bitNum := ((meta >> 8) * 8) + (meta & 0xFF)
		length = int((bitNum + 7) / 8)
		return buf.Next(length)

	case packet.MySQLTypeTimestamp:
		sec, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		return mysql.NewTimestampUnix(int64(sec)*1e6, e.Location), nil

	case packet.MySQLTypeTimestamp2:
		sec, err := buf.BUint32()
		if err != nil {
			return nil, err
		}

		var usec int64
		switch meta {
		case 0:
			usec = 0
		case 1, 2:
			u, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			usec = int64(u) * 10000
		case 3, 4:
			u, err := buf.BUint16()
			if err != nil {
				return nil, err
			}
			usec = int64(u) * 100
		case 5, 6:
			u, err := buf.BUint24()
			if err != nil {
				return nil, err
			}
			usec = int64(u)
		default:
			return nil, fmt.Errorf("invalid meta %d for %s", meta, packet.MySQLTypeTimestamp2)
		}

		return mysql.NewTimestampUnix(int64(sec)*1e6+usec, e.Location), nil

	case packet.MySQLTypeDatetime:
		i64, err := buf.Uint64()
		if err != nil {
			return nil, err
		}

		d := i64 / 1000000
		t := i64 % 1000000

		return mysql.NewDateTime(
			int(d/10000),
			int((d%10000)/100),
			int(d%100),
			int(t/10000),
			int((t%10000)/100),
			int(t%100), 0, e.Location), nil

	case packet.MySQLTypeDatetime2:
		u, err := buf.BUint40()
		if err != nil {
			return nil, err
		}
		intPart := int64(u) - 0x8000000000

		var frac int64
		switch meta {
		case 0:
			frac = 0
		case 1, 2:
			u, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			frac = int64(u) * 10000
		case 3, 4:
			u, err := buf.BUint16()
			if err != nil {
				return nil, err
			}
			frac = int64(u) * 100
		case 5, 6:
			u, err := buf.BUint24()
			if err != nil {
				return nil, err
			}
			frac = int64(u)
		default:
			return nil, fmt.Errorf("invalid meta %d for %s", meta, packet.MySQLTypeDatetime2)
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

		return mysql.NewDateTime(int(year), int(month), int(day), int(hour), int(minute), int(second), int(frac), e.Location), nil

	case packet.MySQLTypeTime:
		i32, err := buf.Uint24()
		if err != nil {
			return nil, err
		}

		return mysql.NewTime(false,
			int(i32/10000),
			int((i32%10000)/100),
			int(i32%100), 0), nil

	case packet.MySQLTypeTime2:
		// TODO parse
		//var intPart ,frac int64
		//switch meta {
		//case 0:
		//
		//}
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeDate, packet.MySQLTypeNewDate:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeYear:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeEnum:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeSet:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeBlob:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeVarchar, packet.MySQLTypeVarString, packet.MySQLTypeString:
		if realType != packet.MySQLTypeString {
			length = int(meta)
		}

		var dataLen int
		if length < 256 {
			u, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			dataLen = int(u)
		} else {
			u, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			dataLen = int(u)
		}

		return buf.Next(dataLen)
	case packet.MysSQLTypeJson:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	case packet.MySQLTypeGeometry:
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	default:
		return nil, fmt.Errorf("unsupported column type: %s", realType)
	}
}
