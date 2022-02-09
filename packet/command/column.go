package command

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
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

	// TODO command was COM_FIELD_LIST
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

func (p *ColumnDefinition) Dump(capabilities generic.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer

	if p.Catalog == "" {
		p.Catalog = "def"
	}
	payload.Write(types.LengthEncodedString.Dump([]byte(p.Catalog)))

	payload.Write(types.LengthEncodedString.Dump([]byte(p.Schema)))
	payload.Write(types.LengthEncodedString.Dump([]byte(p.Table)))
	payload.Write(types.LengthEncodedString.Dump([]byte(p.OrgTable)))
	payload.Write(types.LengthEncodedString.Dump([]byte(p.Name)))
	payload.Write(types.LengthEncodedString.Dump([]byte(p.OrgName)))

	if p.NextLength == 0 {
		p.NextLength = 0x0c
	}
	payload.Write(types.LengthEncodedInteger.Dump(p.NextLength))

	payload.Write(types.FixedLengthInteger.Dump(uint64(p.CharacterSet.Id), 2))
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.ColumnLength), 4))
	payload.WriteByte(byte(p.ColumnType))
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.Flags), 2))
	payload.WriteByte(p.Decimals)

	payload.Write([]byte{0x00, 0x00})

	p.Length = uint32(payload.Len())

	dump := make([]byte, 3+1+p.Length)
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	copy(dump, headerDump)
	copy(dump[4:], payload.Bytes())

	return dump, nil
}
