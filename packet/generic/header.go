package generic

import (
	"bytes"
	"errors"
	"mysql-protocol/packet/types"
)

var (
	ErrPacketData = errors.New("packet data error")
)

type Packet interface {
	SetSequence(int)
	Dump() []byte
}

type Header struct {
	Length uint32
	Seq    uint8
}

func (h *Header) Parse(buf *bytes.Buffer) error {
	// Length
	h.Length = uint32(types.FixedLengthInteger.Get(buf.Next(3)))

	// Sequence
	if buf.Len() == 0 {
		return ErrPacketData
	}
	h.Seq = buf.Next(1)[0]
	return nil
}

func (h *Header) SetSequence(seq int)  {
	h.Seq = uint8(seq)
}

func (h *Header) Dump() []byte {
	bs := types.FixedLengthInteger.Dump(uint64(h.Length), 3)
	bs = append(bs, types.FixedLengthInteger.Dump(uint64(h.Seq), 1)...)
	return bs
}

type Simple struct {
	Header
	Data []byte
}

func NewSimple(data []byte) *Simple {
	return &Simple{
		Data: data,
	}
}

func (p *Simple) Dump() []byte {
	p.Header.Length = uint32(len(p.Data))
	return append(p.Header.Dump(), p.Data...)
}

// ParseGeneric
// TODO 去掉？
func ParseGeneric(bs []byte, capabilities uint32) (Packet, error) {
	l := len(bs)
	if l < 5 {
		return nil, ErrPacketData
	}
	header := bs[4]

	switch {
	// OK Packet
	case header == 0x00 && l > 7:
		return ParseOk(bs, capabilities)
	// EOF Packet
	case header == 0xfe && l < 9:
		return ParseEOF(bs, capabilities)
	// ERR Packet
	case header == 0xff:
		return ParseERR(bs, capabilities)
	default:
		return nil, ErrPacketData
	}
}
