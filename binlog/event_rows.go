package binlog

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"math"
	"strings"
	"time"
)

type TableMapEvent struct {
	EventHeader
	TableId  uint64
	Flags    TableMapFlag
	Database string
	Table    string
	Columns  []Column
	// TODO delete  this?
	OptionalMetadata []byte
	JsonColumnCnt    int
}

type TableMapFlag uint16

const (
	TableMapFlagNoFlags     TableMapFlag = 0
	TableMapFlagBitLenExact TableMapFlag = 1 << (iota - 1)
	TableMapFlagReferredFKDB
)

type Column struct {
	Index int

	BinlogType   flag.TableColumnType
	RealType     flag.TableColumnType
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
	return c.RealType == flag.MySQLTypeGeometry
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
		e.Columns[i].BinlogType = flag.TableColumnType(columnType)
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

	// Count the number of json columns.
	cnt := 0
	for _, column := range e.Columns {
		if column.RealType == flag.MySQLTypeJson {
			cnt++
		}
	}
	e.JsonColumnCnt = cnt

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

		if column.BinlogType == flag.MySQLTypeTypedArray {
			column.IsArray = true
			column.BinlogType = flag.TableColumnType(metaData[pos])
			pos++
		}

		switch column.BinlogType {
		case flag.MySQLTypeFloat,
			flag.MySQLTypeDouble,
			flag.MySQLTypeTime2,
			flag.MySQLTypeTimestamp2,
			flag.MySQLTypeDatetime2,
			flag.MySQLTypeBlob,
			flag.MySQLTypeGeometry,
			flag.MySQLTypeJson:

			// These types store a single byte.
			column.Meta = uint64(metaData[pos])
			pos++

		case flag.MySQLTypeString:
			meta := uint64(metaData[pos]) << 8
			meta += uint64(metaData[pos+1])
			pos += 2
			column.Meta = meta

		case flag.MySQLTypeBit:
			meta := uint64(metaData[pos])
			meta += uint64(metaData[pos+1]) << 8
			pos += 2
			column.Meta = meta

		case flag.MySQLTypeVarchar:
			if column.IsArray {
				// TODO replace method
				column.Meta = packet.FixedLengthInteger.Uint64(metaData[pos : pos+3])
				pos += 3
			} else {
				// TODO replace method
				column.Meta = packet.FixedLengthInteger.Uint64(metaData[pos : pos+2])
				pos += 2
			}

		case flag.MySQLTypeNewDecimal:
			meta := uint64(metaData[pos]) << 8
			meta += uint64(metaData[pos+1])
			pos += 2
			column.Meta = meta

		case flag.MySQLTypeDecimal,
			flag.MySQLTypeTiny,
			flag.MySQLTypeShort,
			flag.MySQLTypeInt24,
			flag.MySQLTypeLong,
			flag.MySQLTypeLongLong,
			flag.MySQLTypeTimestamp,
			flag.MySQLTypeYear,
			flag.MySQLTypeDate,
			flag.MySQLTypeTime,
			flag.MySQLTypeDatetime:

			// These types have no meta.

		case flag.MySQLTypeEnum,
			flag.MySQLTypeSet,
			flag.MySQLTypeTinyBlob,
			flag.MySQLTypeMediumBlob,
			flag.MySQLTypeLongBlob,
			flag.MySQLTypeVarString:

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
		case flag.MySQLTypeString:
			column.RealType = column.BinlogType
			if t := flag.TableColumnType(column.Meta >> 8); isEnumSetType(t) {
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

		if isEnum && column.RealType == flag.MySQLTypeEnum || !isEnum && column.RealType == flag.MySQLTypeSet {
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

		if column.RealType == flag.MySQLTypeGeometry {
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
	ColumnsBeforeImage *core.BitSet
	ColumnsAfterImage  *core.BitSet

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

	// TODO delete TableMap from parser map

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

func (e *RowsEvent) parseRow(buf *mysql.Buffer, isUpdateAfter bool) (err error) {
	columnBits := e.ColumnsBeforeImage
	if isUpdateAfter {
		columnBits = e.ColumnsAfterImage
	}

	// Read value_options if this is AI for EventTypePartialUpdateRows.
	var valueOptions flag.BinlogRowValueOptions
	var partialBits *core.BitSet
	if e.EventType == EventTypePartialUpdateRows && isUpdateAfter {
		u64, err := buf.LengthEncodedUint64()
		if err != nil {
			return err
		}
		valueOptions = flag.BinlogRowValueOptions(u64)

		// Store JSON updates in partial form.
		if valueOptions&flag.BinlogRowValueOptionPartialJsonUpdates != 0 {
			if partialBits, err = buf.CreateBitmap(e.Table.JsonColumnCnt); err != nil {
				return err
			}
		}
	}

	nullBits, err := buf.CreateBitmap(columnBits.Count())
	if err != nil {
		return err
	}

	var row Row
	index := 0
	partialIndex := 0
	for i := 0; i < int(e.ColumnCnt); i++ {
		if !columnBits.Get(i) {
			continue
		}

		isPartial := valueOptions&flag.BinlogRowValueOptionPartialJsonUpdates != 0 && isUpdateAfter &&
			e.Table.Columns[i].RealType == flag.MySQLTypeJson && partialBits.Get(partialIndex)
		partialIndex++

		cv := ColumnValue{ColumnIndex: i}
		isNull := nullBits.Get(index)
		index++

		if isNull {
			cv.IsNull = true
		} else {
			value, err := e.parseColumnValue(buf, i, isPartial)
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

func (e *RowsEvent) parseColumnValue(buf *mysql.Buffer, index int, isPartial bool) (interface{}, error) {
	column := e.Table.Columns[index]

	realType := column.RealType
	meta := column.Meta

	var unsigned bool
	if column.HasSignedness() {
		unsigned = column.Unsigned
	}

	var length int
	if realType == flag.MySQLTypeString {
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
	case flag.MySQLTypeLong:
		// TODO test
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int32(u), nil

	case flag.MySQLTypeTiny:
		// TODO test
		u, err := buf.Uint8()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int8(u), nil

	case flag.MySQLTypeShort:
		// TODO test
		u, err := buf.Uint16()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int16(u), nil

	case flag.MySQLTypeInt24:
		// TODO test
		u, err := buf.Uint24()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int32(u), nil

	case flag.MySQLTypeLongLong:
		// TODO test
		u, err := buf.Uint64()
		if err != nil {
			return nil, err
		}
		if unsigned {
			return u, nil
		}
		return int64(u), nil

	case flag.MySQLTypeNewDecimal:
		precision := int(meta >> 8)
		decimals := int(meta & 0xFF)
		return buf.Decimal(precision, decimals)

	case flag.MySQLTypeFloat:
		// TODO test
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(u), nil

	case flag.MySQLTypeDouble:
		// TODO test
		u, err := buf.Uint64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(u), nil

	case flag.MySQLTypeBit:
		// TODO test
		// TODO convert to int?
		bitNum := ((meta >> 8) * 8) + (meta & 0xFF)
		length = int((bitNum + 7) / 8)
		return buf.Next(length)

	case flag.MySQLTypeTimestamp:
		// TODO test
		sec, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		return core.NewTimestampUnix(int64(sec)*1e6, e.Location), nil

	case flag.MySQLTypeTimestamp2:
		// TODO test
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
			return nil, fmt.Errorf("invalid meta %d for %s", meta, flag.MySQLTypeTimestamp2)
		}

		return core.NewTimestampUnix(int64(sec)*1e6+usec, e.Location), nil

	case flag.MySQLTypeDatetime:
		// TODO test
		i64, err := buf.Uint64()
		if err != nil {
			return nil, err
		}

		d := i64 / 1000000
		t := i64 % 1000000

		return core.NewDateTime(
			int(d/10000),
			int((d%10000)/100),
			int(d%100),
			int(t/10000),
			int((t%10000)/100),
			int(t%100), 0, e.Location), nil

	case flag.MySQLTypeDatetime2:
		// TODO test
		u, err := buf.BUint40()
		if err != nil {
			return nil, err
		}

		// On disk we store as unsigned number with offset.
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
			return nil, fmt.Errorf("invalid meta %d for %s", meta, flag.MySQLTypeDatetime2)
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

		return core.NewDateTime(int(year), int(month), int(day), int(hour), int(minute), int(second), int(frac), e.Location), nil

	case flag.MySQLTypeTime:
		// TODO test
		i32, err := buf.Uint24()
		if err != nil {
			return nil, err
		}

		return core.NewTime(false,
			int(i32/10000),
			int((i32%10000)/100),
			int(i32%100), 0), nil

	case flag.MySQLTypeTime2:
		// TODO test
		// TODO parse

		// On disk we convert from signed representation to unsigned
		// representation using timeFIntOffset, so all values become binary comparable.
		var timeFIntOffset int64 = 0x800000

		var intPart, frac, tmp int64
		switch meta {
		case 0:
			u, err := buf.BUint24()
			if err != nil {
				return nil, err
			}
			intPart = int64(u) - timeFIntOffset
			tmp = intPart << 24
		case 1, 2:
			u, err := buf.BUint24()
			if err != nil {
				return nil, err
			}
			intPart = int64(u) - timeFIntOffset
			u2, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			frac = int64(u2)
			if intPart < 0 && frac > 0 {
				/*
				   Negative values are stored with
				   reverse fractional part order,
				   for binary sort compatibility.

				    Disk value  intpart frac   Time value   Memory value
				    800000.00    0      0      00:00:00.00  0000000000.000000
				    7FFFFF.FF   -1      255   -00:00:00.01  FFFFFFFFFF.FFD8F0
				    7FFFFF.9D   -1      99    -00:00:00.99  FFFFFFFFFF.F0E4D0
				    7FFFFF.00   -1      0     -00:00:01.00  FFFFFFFFFF.000000
				    7FFFFE.FF   -1      255   -00:00:01.01  FFFFFFFFFE.FFD8F0
				    7FFFFE.F6   -2      246   -00:00:01.10  FFFFFFFFFE.FE7960

				    Formula to convert fractional part from disk format
				    (now stored in "frac" variable) to absolute value: "0x100 - frac".
				    To reconstruct in-memory value, we shift
				    to the next integer value and then substruct fractional part.
				*/

				// Shift to the next integer value
				intPart++
				frac -= 0x100
			}
			tmp = intPart<<24 + frac*10000
		case 3, 4:
			u, err := buf.BUint24()
			if err != nil {
				return nil, err
			}
			intPart = int64(u) - timeFIntOffset
			u2, err := buf.BUint16()
			if err != nil {
				return nil, err
			}
			frac = int64(u2)
			if intPart < 0 && frac > 0 {
				// Fix reverse fractional part order: "0x10000 - frac".
				// See comments for FSP=1 and FSP=2 above.

				// Shift to the next integer value
				intPart++
				frac -= 0x10000
			}
			tmp = intPart<<24 + frac*100
		case 5, 6:
			u, err := buf.BUint48()
			if err != nil {
				return nil, err
			}
			intPart = int64(u) - timeFIntOffset
			tmp = intPart
		default:
			return nil, fmt.Errorf("invalid meta %d for %s", meta, flag.MySQLTypeTime2)
		}

		var neg bool
		if tmp < 0 {
			neg = true
			tmp = -tmp
		}

		hms := tmp >> 24
		hour := (hms >> 12) % (1 << 10)
		minute := (hms >> 6) % (1 << 6)
		second := hms % (1 << 6)
		usec := tmp % (1 << 24)

		return core.NewTime(neg, int(hour), int(minute), int(second), int(usec)), nil

	// todo only MySQLTypeDate in binlog, delete MySQLTypeNewDate or change realType to MySQLTypeNewDate?
	case flag.MySQLTypeDate, flag.MySQLTypeNewDate:
		// TODO test
		u32, err := buf.Uint24()
		if err != nil {
			return nil, err
		}

		year := u32 >> 9
		month := u32 >> 5 % 16
		day := u32 % 32

		return core.NewDate(int(year), int(month), int(day)), nil

	case flag.MySQLTypeYear:
		// TODO test
		u8, err := buf.Uint8()
		if err != nil {
			return nil, err
		}
		return uint16(u8) + 1900, nil

	case flag.MySQLTypeEnum:
		// TODO test
		switch meta & 0xff {
		case 1:
			return buf.Uint8()
		case 2:
			return buf.Uint16()
		default:
			return nil, fmt.Errorf("unknown ENUM packlen=%d", meta&0xff)
		}

	case flag.MySQLTypeSet:
		// TODO test
		switch meta & 0xff {
		case 1:
			return buf.Uint8()
		case 2:
			return buf.Uint16()
		case 3:
			return buf.Uint24()
		case 4:
			return buf.Uint32()
		case 5:
			return buf.Uint40()
		case 6:
			return buf.Uint48()
		case 7:
			return buf.Uint56()
		case 8:
			return buf.Uint64()
		default:
			return nil, fmt.Errorf("unknown SET packlen=%d", meta&0xff)
		}

	case flag.MySQLTypeBlob:
		// TODO test
		var l int
		switch meta {
		case 1:
			// TINYBLOB/TINYTEXT
			u8, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			l = int(u8)
		case 2:
			// BLOB/TEXT
			u16, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			l = int(u16)
		case 3:
			// MEDIUMBLOB/MEDIUMTEXT
			u24, err := buf.Uint24()
			if err != nil {
				return nil, err
			}
			l = int(u24)
		case 4:
			// LONGBLOB/LONGTEXT
			u32, err := buf.Uint32()
			if err != nil {
				return nil, err
			}
			l = int(u32)
		}

		return buf.Next(l)

	case flag.MySQLTypeVarchar, flag.MySQLTypeVarString:
		// TODO test
		length = int(meta)
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

	case flag.MySQLTypeString:
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

	case flag.MySQLTypeJson:
		// TODO test

		// TODO meta must be 4?
		if meta != 4 {
			return nil, fmt.Errorf("require json meta=4")
		}
		u32, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		length = int(u32)

		data := buf.Buffer()
		if _, err := buf.Next(length); err != nil {
			return nil, err
		}

		if isPartial {
			// TODO json diff
			panic("TODO json diff")
		} else {
			if length == 0 {
				return "", nil
			}
			jsonValue, err := ParseBinary(data, length)
			if err != nil {
				return nil, err
			}
			sb := new(strings.Builder)
			err = jsonValue.WriteStringBuilder(sb, e.Location)
			return sb.String(), err
		}

	case flag.MySQLTypeGeometry:
		// TODO test
		// TODO parse
		return nil, fmt.Errorf("todo parse time")

	default:
		return nil, fmt.Errorf("unsupported column type: %s", realType)
	}
}
