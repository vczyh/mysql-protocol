package packet

import (
	"bytes"
	"errors"
	"github.com/vczyh/mysql-protocol/core"
)

var (
	ErrPacketData = errors.New("packet: data error")
)

type Packet interface {
	SetSequence(int)
	Dump(core.CapabilityFlag) ([]byte, error)
}

type Header struct {
	Length uint32
	Seq    uint8
}

func (h *Header) Parse(buf *bytes.Buffer) error {
	// Length
	h.Length = uint32(FixedLengthInteger.Get(buf.Next(3)))

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

func (h *Header) Dump(core.CapabilityFlag) ([]byte, error) {
	bs := FixedLengthInteger.Dump(uint64(h.Length), 3)
	bs = append(bs, FixedLengthInteger.Dump(uint64(h.Seq), 1)...)
	return bs, nil
}
