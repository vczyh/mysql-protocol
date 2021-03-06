package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/myerrors"
)

// ERR https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
type ERR struct {
	ERRHeader      uint8 // 0xff
	ErrorCode      code.Err
	SqlStateMarker byte // 0x23
	SqlState       string
	ErrorMessage   string
}

func NewERR(err error) *ERR {
	return &ERR{
		ERRHeader:      0xff,
		ErrorCode:      myerrors.Code(err),
		SqlStateMarker: 0x23,
		SqlState:       myerrors.SQLState(err),
		ErrorMessage:   myerrors.Message(err),
	}

}

func ParseERR(bs []byte, capabilities flag.Capability) (*ERR, error) {
	p := new(ERR)

	buf := bytes.NewBuffer(bs)

	// ERR Header
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.ERRHeader = buf.Next(1)[0]

	// Error Err
	p.ErrorCode = code.Err(FixedLengthInteger.Get(buf.Next(2)))

	if capabilities&flag.ClientProtocol41 != 0 {
		if buf.Len() == 0 {
			return nil, ErrPacketData
		}
		// SQL State Marker
		p.SqlStateMarker = buf.Next(1)[0]
		// SQL State
		p.SqlState = string(buf.Next(5))
	}

	// Error Message
	p.ErrorMessage = buf.String()

	return p, nil
}

func (e *ERR) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer
	// ERR Header
	payload.WriteByte(e.ERRHeader)

	// Error Err
	payload.Write(FixedLengthInteger.Dump(uint64(e.ErrorCode), 2))

	if capabilities&flag.ClientProtocol41 != 0 {
		payload.WriteByte(e.SqlStateMarker)
		payload.WriteString(e.SqlState)
	}

	payload.WriteString(e.ErrorMessage)

	return payload.Bytes(), nil
}

func (e *ERR) Error() string {
	return fmt.Sprintf("ERROR %d (%s): %s", e.ErrorCode, e.SqlState, e.ErrorMessage)
}
