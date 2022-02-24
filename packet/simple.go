package packet

import (
	"github.com/vczyh/mysql-protocol/flag"
)

type Simple struct {
	Header
	Payload []byte
}

func NewSimple(payload []byte) *Simple {
	return &Simple{
		Payload: payload,
	}
}

func (p *Simple) Dump(capabilities flag.CapabilityFlag) ([]byte, error) {
	p.Header.Length = uint32(len(p.Payload))
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	return append(headerDump, p.Payload...), nil
}
