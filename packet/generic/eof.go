package generic

import (
	"bytes"
	"mysql-protocol/packet/types"
)

// EOF https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
type EOF struct {
	Header

	EOFHeader    uint8
	WarningCount uint16
	StatusFlags  uint16
}

func ParseEOF(bs []byte, capabilities uint32) (*EOF, error) {
	var p EOF
	var err error

	buf := bytes.NewBuffer(bs)
	// Header
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	// EOF Header
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.EOFHeader = buf.Next(1)[0]

	if capabilities&CLIENT_PROTOCOL_41 != 0x00000000 {
		// Warning Count
		p.WarningCount = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
		// Status Flags
		p.StatusFlags = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
	}

	return &p, nil
}
