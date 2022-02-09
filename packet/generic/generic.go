package generic

import (
	"bytes"
	"errors"
	"github.com/vczyh/mysql-protocol/packet/types"
)

var (
	ErrPacketData = errors.New("packet: data error")
)

type Packet interface {
	SetSequence(int)
	Dump(CapabilityFlag) ([]byte, error)
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

func (h *Header) Dump(CapabilityFlag) ([]byte, error) {
	bs := types.FixedLengthInteger.Dump(uint64(h.Length), 3)
	bs = append(bs, types.FixedLengthInteger.Dump(uint64(h.Seq), 1)...)
	return bs, nil
}

type Simple struct {
	Header
	Payload []byte
}

func NewSimple(payload []byte) *Simple {
	return &Simple{
		Payload: payload,
	}
}

func (p *Simple) Dump(capabilities CapabilityFlag) ([]byte, error) {
	p.Header.Length = uint32(len(p.Payload))
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	return append(headerDump, p.Payload...), nil
}
