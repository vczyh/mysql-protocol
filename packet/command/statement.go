package command

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

type StmtPrepareOKFirst struct {
	generic.Header

	Status       uint8
	StmtId       uint32
	ColumnCount  uint16
	ParamCount   uint16
	WarningCount uint16
}

func ParseStmtPrepareOKFirst(data []byte) (*StmtPrepareOKFirst, error) {
	var p StmtPrepareOKFirst
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	if p.Status, err = buf.ReadByte(); err != nil {
		return nil, err
	}

	p.StmtId = uint32(types.FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnCount = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
	p.ParamCount = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	buf.Next(1)
	p.WarningCount = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	return &p, nil
}

type StmtExecute struct {
	generic.Header

	ComStmtExecute uint8
	StmtId         uint32
	Flags          uint8
	IterationCount uint32
}
