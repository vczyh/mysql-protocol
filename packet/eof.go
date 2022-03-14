package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/flag"
)

// EOF https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
type EOF struct {
	EOFHeader    uint8
	WarningCount uint16
	StatusFlags  flag.Status
}

func NewEOF(warningCount int, statusFlag flag.Status) *EOF {
	return &EOF{
		EOFHeader:    0xfe,
		WarningCount: uint16(warningCount),
		StatusFlags:  statusFlag,
	}
}

func ParseEOF(bs []byte, capabilities flag.Capability) (*EOF, error) {
	p := new(EOF)

	buf := bytes.NewBuffer(bs)

	// EOF Header
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.EOFHeader = buf.Next(1)[0]

	if capabilities&flag.ClientProtocol41 != 0 {
		// Warning Count
		p.WarningCount = uint16(FixedLengthInteger.Get(buf.Next(2)))
		// Status Flags
		p.StatusFlags = flag.Status(FixedLengthInteger.Get(buf.Next(2)))
	}

	return p, nil
}

func (eof *EOF) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer

	payload.WriteByte(eof.EOFHeader)
	if capabilities&flag.ClientProtocol41 != 0 {
		payload.Write(FixedLengthInteger.Dump(uint64(eof.WarningCount), 2))
		payload.Write(FixedLengthInteger.Dump(uint64(eof.StatusFlags), 2))
	}

	return payload.Bytes(), nil
}
