package command

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

type Query struct {
	generic.Header

	QueryHeader uint8
	Query       []byte
}

func (p *Query) Dump() []byte {
	var payload bytes.Buffer

	p.QueryHeader = COM_QUERY
	payload.WriteByte(p.QueryHeader)
	payload.Write(p.Query)

	p.Header.Length = uint32(payload.Len())

	dump := make([]byte, 3+1+p.Header.Length)
	copy(dump, p.Header.Dump())
	copy(dump[4:], payload.Bytes())

	return dump
}

func (p *Query) SetQuery(query string) {
	p.Query = []byte(query)
}

type QueryResponse struct {
	generic.Header
	ColumnCount uint64
}

func ParseQueryResponse(data []byte) (*QueryResponse, error) {
	var p QueryResponse
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	if p.ColumnCount, err = types.LengthEncodedInteger.Get(buf); err != nil {
		return nil, err
	}

	return &p, nil
}
