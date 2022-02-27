package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
)

type SSLRequest struct {
	Header

	ClientCapabilityFlags flag.Capability
	MaxPacketSize         uint32
	CharacterSet          *charset.Collation
}

func ParseSSLRequest(data []byte) (*SSLRequest, error) {
	var p SSLRequest
	var err error

	buf := bytes.NewBuffer(data)
	// Header
	if err = p.Parse(buf); err != nil {
		return nil, err
	}

	// Client Capability Flags
	p.ClientCapabilityFlags = flag.Capability(uint32(FixedLengthInteger.Get(buf.Next(4))))

	// Max Packet Size
	p.MaxPacketSize = uint32(FixedLengthInteger.Get(buf.Next(4)))

	// Character Set
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	collationId := buf.Next(1)[0]
	collation, ok := charset.CollationIds[collationId]
	if !ok {
		return nil, fmt.Errorf("unknown collation id %d", collationId)
	}
	p.CharacterSet = collation

	// Reserved
	buf.Next(23)

	return &p, nil
}

func (p *SSLRequest) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer
	payload.Write(FixedLengthInteger.Dump(uint64(p.ClientCapabilityFlags), 4))

	// Max Packet Size
	payload.Write(FixedLengthInteger.Dump(uint64(p.MaxPacketSize), 4))

	// Character Set
	payload.Write(FixedLengthInteger.Dump(uint64(p.CharacterSet.Id), 1))

	// Reserved
	for i := 0; i < 23; i++ {
		payload.WriteByte(0x00)
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
