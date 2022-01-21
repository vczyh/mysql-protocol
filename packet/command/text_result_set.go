package command

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"math"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
	"time"
)

type TableColumnType uint8

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
// https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
const (
	MYSQL_TYPE_DECIMAL TableColumnType = iota
	MYSQL_TYPE_TINY
	MYSQL_TYPE_SHORT
	MYSQL_TYPE_LONG
	MYSQL_TYPE_FLOAT
	MYSQL_TYPE_DOUBLE
	MYSQL_TYPE_NULL
	MYSQL_TYPE_TIMESTAMP
	MYSQL_TYPE_LONGLONG
	MYSQL_TYPE_INT24
	MYSQL_TYPE_DATE
	MYSQL_TYPE_TIME
	MYSQL_TYPE_DATETIME
	MYSQL_TYPE_YEAR
	MYSQL_TYPE_NEWDATE
	MYSQL_TYPE_VARCHAR
	MYSQL_TYPE_BIT
	MYSQL_TYPE_TIMESTAMP2
	MYSQL_TYPE_DATETIME2
	MYSQL_TYPE_TIME2
	MYSQL_TYPE_NEWDECIMAL = iota + 0xe2
	MYSQL_TYPE_ENUM
	MYSQL_TYPE_SET
	MYSQL_TYPE_TINY_BLOB
	MYSQL_TYPE_MEDIUM_BLOB
	MYSQL_TYPE_LONG_BLOB
	MYSQL_TYPE_BLOB
	MYSQL_TYPE_VAR_STRING
	MYSQL_TYPE_STRING
	MYSQL_TYPE_GEOMETRY
)

func (t TableColumnType) String() string {
	switch t {
	case MYSQL_TYPE_DECIMAL:
		return "MYSQL_TYPE_DECIMAL"
	case MYSQL_TYPE_TINY:
		return "MYSQL_TYPE_TINY"
	case MYSQL_TYPE_SHORT:
		return "MYSQL_TYPE_SHORT"
	case MYSQL_TYPE_LONG:
		return "MYSQL_TYPE_LONG"
	case MYSQL_TYPE_FLOAT:
		return "MYSQL_TYPE_FLOAT"
	case MYSQL_TYPE_DOUBLE:
		return "MYSQL_TYPE_DOUBLE"
	case MYSQL_TYPE_NULL:
		return "MYSQL_TYPE_NULL"
	case MYSQL_TYPE_TIMESTAMP:
		return "MYSQL_TYPE_TIMESTAMP"
	case MYSQL_TYPE_LONGLONG:
		return "MYSQL_TYPE_LONGLONG"
	case MYSQL_TYPE_INT24:
		return "MYSQL_TYPE_INT24"
	case MYSQL_TYPE_DATE:
		return "MYSQL_TYPE_DATE"
	case MYSQL_TYPE_TIME:
		return "MYSQL_TYPE_TIME"
	case MYSQL_TYPE_DATETIME:
		return "MYSQL_TYPE_DATETIME"
	case MYSQL_TYPE_YEAR:
		return "MYSQL_TYPE_YEAR"
	case MYSQL_TYPE_NEWDATE:
		return "MYSQL_TYPE_NEWDATE"
	case MYSQL_TYPE_VARCHAR:
		return "MYSQL_TYPE_VARCHAR"
	case MYSQL_TYPE_BIT:
		return "MYSQL_TYPE_BIT"
	case MYSQL_TYPE_TIMESTAMP2:
		return "MYSQL_TYPE_TIMESTAMP2"
	case MYSQL_TYPE_DATETIME2:
		return "MYSQL_TYPE_DATETIME2"
	case MYSQL_TYPE_TIME2:
		return "MYSQL_TYPE_TIME2"
	case MYSQL_TYPE_NEWDECIMAL:
		return "MYSQL_TYPE_NEWDECIMAL"
	case MYSQL_TYPE_ENUM:
		return "MYSQL_TYPE_ENUM"
	case MYSQL_TYPE_SET:
		return "MYSQL_TYPE_SET"
	case MYSQL_TYPE_TINY_BLOB:
		return "MYSQL_TYPE_TINY_BLOB"
	case MYSQL_TYPE_MEDIUM_BLOB:
		return "MYSQL_TYPE_MEDIUM_BLOB"
	case MYSQL_TYPE_LONG_BLOB:
		return "MYSQL_TYPE_LONG_BLOB"
	case MYSQL_TYPE_BLOB:
		return "MYSQL_TYPE_BLOB"
	case MYSQL_TYPE_VAR_STRING:
		return "MYSQL_TYPE_VAR_STRING"
	case MYSQL_TYPE_STRING:
		return "MYSQL_TYPE_STRING"
	case MYSQL_TYPE_GEOMETRY:
		return "MYSQL_TYPE_GEOMETRY"
	default:
		return "Unknown TableColumnType"
	}
}

type ColumnDefinition struct {
	generic.Header

	Catalog      []byte
	Schema       []byte
	Table        []byte
	OrgTable     []byte
	Name         []byte
	OrgName      []byte
	NextLength   uint64
	CharacterSet uint16
	ColumnLength uint32
	ColumnType   uint8
	Flags        uint16
	Decimals     uint8
	DefaultValue []byte
}

func ParseColumnDefinition(bs []byte) (*ColumnDefinition, error) {
	var p ColumnDefinition
	var err error

	buf := bytes.NewBuffer(bs)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.Catalog, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.Schema, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.Table, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.OrgTable, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.Name, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.OrgName, err = types.LengthEncodedString.Get(buf)
	if err != nil {
		return nil, err
	}

	p.NextLength, err = types.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, err
	}

	p.CharacterSet = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
	p.ColumnLength = uint32(types.FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnType = uint8(types.FixedLengthInteger.Get(buf.Next(1)))
	p.Flags = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
	p.Decimals = uint8(types.FixedLengthInteger.Get(buf.Next(1)))

	// filler [00] [00]
	buf.Next(2)

	return &p, nil
}

type TextResultSetRow struct {
	generic.Header
	ColumnValues [][]byte
}

func ParseTextResultSetRow(bs []byte) (*TextResultSetRow, error) {
	var p TextResultSetRow
	var err error

	buf := bytes.NewBuffer(bs)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	for buf.Len() > 0 {
		rowData := buf.Bytes()
		if val := rowData[0]; val == 0xfb {
			p.ColumnValues = append(p.ColumnValues, []byte{val})
			buf = bytes.NewBuffer(rowData[1:])
		} else {
			buf = bytes.NewBuffer(rowData)
			val, err := types.LengthEncodedString.Get(buf)
			if err != nil {
				return nil, generic.ErrPacketData
			}
			p.ColumnValues = append(p.ColumnValues, val)
		}
	}

	return &p, nil
}

type Value string

func (v *Value) IsNull() bool {
	return v == nil
}

func (v *Value) String() string {
	if v.IsNull() {
		return "<null>"
	}
	return string(*v)
}

func (p *TextResultSetRow) GetValues() (values []*Value) {
	for _, columnVal := range p.ColumnValues {
		var val *Value
		if len(columnVal) == 1 && columnVal[0] == 0xfb {
			// Null
		} else {
			v := Value(columnVal)
			val = &v
		}
		values = append(values, val)
	}
	return values
}

type BinaryResultSetRow struct {
	generic.Header

	PktHeader  byte
	NullBitMap []byte
	Values     []byte
}

func ParseBinaryResultSetRow(data []byte, columnCount int) (*BinaryResultSetRow, error) {
	var p BinaryResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	if p.PktHeader, err = buf.ReadByte(); err != nil {
		return nil, err
	}

	nullBitMapLen := (columnCount + 7 + 2) >> 3
	p.NullBitMap = buf.Next(nullBitMapLen)

	p.Values = buf.Bytes()

	return &p, nil
}

func (p *BinaryResultSetRow) NullBitMapGet(columnCount, index int) bool {
	if p.NullBitMap == nil {
		return false
	}
	offset := 2
	bytePos := (index + offset) >> 3
	bitPos := (index + offset) % 8
	return (p.NullBitMap[bytePos]>>bitPos)&1 != 0
}

// Convert https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
func (p *BinaryResultSetRow) Convert(dest []driver.Value, columns []*ColumnDefinition, loc *time.Location) error {
	buf := bytes.NewBuffer(p.Values)

	for i := range dest {
		if p.NullBitMapGet(len(dest), i) {
			dest[i] = nil
			continue
		}

		flags := generic.ColumnDefinitionFlag(columns[i].Flags)
		columnType := TableColumnType(columns[i].ColumnType)
		switch columnType {
		case MYSQL_TYPE_TINY:
			val := types.FixedLengthInteger.Get(buf.Next(1))
			if flags&generic.UNSIGNED_FLAG != 0 {
				dest[i] = uint8(val)
			} else {
				dest[i] = int8(val)
			}

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			val := types.FixedLengthInteger.Get(buf.Next(2))
			if flags&generic.UNSIGNED_FLAG != 0 {
				dest[i] = uint16(val)
			} else {
				dest[i] = int16(val)
			}

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			val := types.FixedLengthInteger.Get(buf.Next(4))
			if flags&generic.UNSIGNED_FLAG != 0 {
				dest[i] = uint32(val)
			} else {
				dest[i] = int32(val)
			}

		case MYSQL_TYPE_LONGLONG:
			val := types.FixedLengthInteger.Get(buf.Next(8))
			if flags&generic.UNSIGNED_FLAG != 0 {
				dest[i] = val
			} else {
				dest[i] = int64(val)
			}

		case MYSQL_TYPE_FLOAT:
			dest[i] = math.Float32frombits(uint32(types.FixedLengthInteger.Get(buf.Next(4))))

		case MYSQL_TYPE_DOUBLE:
			dest[i] = math.Float64frombits(types.FixedLengthInteger.Get(buf.Next(8)))

		case MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT,
			MYSQL_TYPE_ENUM,
			MYSQL_TYPE_SET,
			MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
			data, err := types.LengthEncodedString.Get(buf)
			if err != nil {
				return nil
			}
			dest[i] = data

		case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
			dataLen := types.FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				dest[i] = time.Time{}
				continue
			}

			switch dataLen {
			case 0:
				dest[i] = time.Time{}
			case 4:
				dest[i] = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					0, 0, 0, 0, loc)
			case 7:
				dest[i] = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					0, loc)
			case 11:
				dest[i] = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(4)))*1000,
					loc)
			}

		case MYSQL_TYPE_TIME:
			dest[i] = time.Time{}
		// TODO
		//dataLen := types.FixedLengthInteger.Get(buf.Next(1))
		//if dataLen == 0 {
		//	dest[i] = time.Time{}
		//	continue
		//}
		//
		//switch dataLen {
		//case 0:
		//	dest[i] = time.Time{}
		//case 8:
		//
		//}

		default:
			return fmt.Errorf("not supported mysql type: %s", columnType)
		}
	}

	return nil
}
