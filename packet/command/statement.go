package command

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
)

// StmtPrepareOKFirst https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
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

// StmtExecute https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
type StmtExecute struct {
	generic.Header

	ComStmtExecute     uint8
	StmtId             uint32
	Flags              uint8
	IterationCount     uint32
	NullBitMap         []byte
	NewParamsBoundFlag uint8
	ParamType          []byte
	ParamValue         []byte
}

func NewStmtExecute() *StmtExecute {
	return &StmtExecute{
		IterationCount: 1,
		ComStmtExecute: generic.ComStmtExecute,
	}
}

func (p *StmtExecute) CreateNullBitMap(paramCount int) {
	if p.NullBitMap == nil {
		offset := 0
		p.NullBitMap = make([]byte, (paramCount+7+offset)/8)
	}
}

func (p *StmtExecute) NullBitMapSet(paramCount, index int) {
	if p.NullBitMap == nil {
		p.CreateNullBitMap(paramCount)
	}
	offset := 0
	bytePos := (index + offset) / 8
	bitPos := (index + offset) % 8
	p.NullBitMap[bytePos] |= 1 << bitPos
}

func (p *StmtExecute) Dump(capabilities generic.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(p.ComStmtExecute)
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.StmtId), 4))
	payload.WriteByte(p.Flags)
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.IterationCount), 4))

	if p.NullBitMap != nil {
		payload.Write(p.NullBitMap)
		payload.WriteByte(p.NewParamsBoundFlag)
	}

	if p.NewParamsBoundFlag == 1 {
		payload.Write(p.ParamType)
		payload.Write(p.ParamValue)
	}

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
