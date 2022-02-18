package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/core"
)

func New(cmd core.Command, data []byte) *Simple {
	data = append([]byte{byte(cmd)}, data...)
	return NewSimple(data)
}

func ParseColumnCount(data []byte) (uint64, error) {
	if len(data) < 5 {
		return 0, ErrPacketData
	}
	buf := bytes.NewBuffer(data[4:])
	columnCount, err := LengthEncodedInteger.Get(buf)
	return columnCount, err
}

func NewColumnCount(count int) (Packet, error) {
	return NewSimple(LengthEncodedInteger.Dump(uint64(count))), nil
}
