package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/flag"
)

// StmtPrepareOKFirst https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
type StmtPrepareOKFirst struct {
	Status       uint8
	StmtId       uint32
	ColumnCount  uint16
	ParamCount   uint16
	WarningCount uint16
}

func ParseStmtPrepareOKFirst(data []byte) (p *StmtPrepareOKFirst, err error) {
	p = new(StmtPrepareOKFirst)

	buf := bytes.NewBuffer(data)
	if p.Status, err = buf.ReadByte(); err != nil {
		return nil, err
	}

	p.StmtId = uint32(FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnCount = uint16(FixedLengthInteger.Get(buf.Next(2)))
	p.ParamCount = uint16(FixedLengthInteger.Get(buf.Next(2)))

	buf.Next(1)
	p.WarningCount = uint16(FixedLengthInteger.Get(buf.Next(2)))

	return p, nil
}

// StmtExecute https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
type StmtExecute struct {
	ComStmtExecute     uint8
	StmtId             uint32
	Flags              uint8
	IterationCount     uint32
	NullBitMap         []byte
	NewParamsBoundFlag uint8
	ParamType          []byte
	ParamValue         []byte
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

func (p *StmtExecute) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(p.ComStmtExecute)
	payload.Write(FixedLengthInteger.Dump(uint64(p.StmtId), 4))
	payload.WriteByte(p.Flags)
	payload.Write(FixedLengthInteger.Dump(uint64(p.IterationCount), 4))

	if p.NullBitMap != nil {
		payload.Write(p.NullBitMap)
		payload.WriteByte(p.NewParamsBoundFlag)
	}

	if p.NewParamsBoundFlag == 1 {
		payload.Write(p.ParamType)
		payload.Write(p.ParamValue)
	}

	return payload.Bytes(), nil
}
