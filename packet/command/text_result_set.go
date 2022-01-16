package command

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

// ResultSet https://dev.mysql.com/doc/internals/en/com-query-response.html
type ResultSet struct {
	generic.Header

	ColumnCount uint64
}

func ParseResultSet(bs []byte) (*ResultSet, error) {
	var p ResultSet
	var err error

	buf := bytes.NewBuffer(bs)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.ColumnCount, err = types.LengthEncodedInteger.Get(buf)
	if err != nil {
		return nil, generic.ErrPacketData
	}
	return nil, nil
}

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

	// todo
	// if command was COM_FIELD_LIST {
	// 		lenenc_int     length of default-values
	// 		string[$len]   default values
	// }

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

func (p *TextResultSetRow) GetValues() (values []string) {
	for _, val := range p.ColumnValues {
		values = append(values, string(val))
	}
	return values
}
