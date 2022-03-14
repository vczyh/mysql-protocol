package packet

import (
	"errors"
	"github.com/vczyh/mysql-protocol/flag"
)

var (
	ErrPacketData = errors.New("packet: data error")
)

type Packet interface {
	Dump(flag.Capability) ([]byte, error)
}

type Simple struct {
	data []byte
}

func NewSimple(data []byte) *Simple {
	return &Simple{
		data: data,
	}
}

func (s *Simple) Dump(capabilities flag.Capability) ([]byte, error) {
	return s.data, nil
}
