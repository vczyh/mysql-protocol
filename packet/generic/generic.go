package generic

import (
	"bytes"
	"errors"
	"mysql-protocol/packet/types"
)

var (
	ErrPacketData = errors.New("packet: data error")
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

func (h *Header) SetSequence(seq int) {
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
