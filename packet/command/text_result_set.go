package command

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

type TableColumnType uint8

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
