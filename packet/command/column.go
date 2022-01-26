package command

import (
	"bytes"
	"fmt"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

// ColumnDefinition https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
type ColumnDefinition struct {
	generic.Header

	Catalog      string
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	NextLength   uint64
	CharacterSet *generic.Collation
	ColumnLength uint32
	ColumnType   generic.TableColumnType
	Flags        generic.ColumnDefinitionFlag
	Decimals     uint8
}

func ParseColumnDefinition(bs []byte) (*ColumnDefinition, error) {
	var p ColumnDefinition
	var err error

	buf := bytes.NewBuffer(bs)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	var b []byte
	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Catalog = string(b)

	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Schema = string(b)

	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Table = string(b)

	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.OrgTable = string(b)

	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Name = string(b)

	if b, err = types.LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.OrgName = string(b)

	if p.NextLength, err = types.LengthEncodedInteger.Get(buf); err != nil {
		return nil, err
	}

	collationId := uint8(types.FixedLengthInteger.Get(buf.Next(2)))
	collation, ok := generic.CollationIds[collationId]
	if !ok {
		return nil, fmt.Errorf("unknown collation id %d", collationId)
	}
	p.CharacterSet = collation

	p.ColumnLength = uint32(types.FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnType = generic.TableColumnType(types.FixedLengthInteger.Get(buf.Next(1)))
	p.Flags = generic.ColumnDefinitionFlag(types.FixedLengthInteger.Get(buf.Next(2)))
	p.Decimals = uint8(types.FixedLengthInteger.Get(buf.Next(1)))

	// filler [00] [00]
	buf.Next(2)

	return &p, nil
}
