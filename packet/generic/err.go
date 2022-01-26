package generic

import (
	"bytes"
	"mysql-protocol/packet/types"
)

// ERR https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
type ERR struct {
	Header

	ERRHeader      uint8
	ErrorCode      uint16
	SqlStateMarker byte
	SqlState       []byte
	ErrorMessage   []byte
}

func ParseERR(bs []byte, capabilities CapabilityFlag) (*ERR, error) {
	var p ERR
	var err error

	buf := bytes.NewBuffer(bs)
	// Header
	if err = p.Parse(buf); err != nil {
		return nil, err
	}

	// ERR Header
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.ERRHeader = buf.Next(1)[0]

	// Error Code
	p.ErrorCode = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	if capabilities&ClientProtocol41 != 0 {
		if buf.Len() == 0 {
			return nil, ErrPacketData
		}
		// Sql State Marker
		p.SqlStateMarker = buf.Next(1)[0]
		// Sql State
		p.SqlState = buf.Next(5)
	}

	// Error Message
	p.ErrorMessage = buf.Bytes()

	return &p, nil
}

func (e *ERR) Error() string {
	return string(e.ErrorMessage)
}
