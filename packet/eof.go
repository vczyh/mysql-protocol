package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/flag"
)

// EOF https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
type EOF struct {
	Header

	EOFHeader    uint8
	WarningCount uint16
	StatusFlags  flag.StatusFlag
}

func NewEOF(warningCount int, statusFlag flag.StatusFlag) *EOF {
	return &EOF{
		EOFHeader:    0xfe,
		WarningCount: uint16(warningCount),
		StatusFlags:  statusFlag,
	}
}

func ParseEOF(bs []byte, capabilities flag.CapabilityFlag) (*EOF, error) {
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

	if capabilities&flag.ClientProtocol41 != 0 {
		// Warning Count
		p.WarningCount = uint16(FixedLengthInteger.Get(buf.Next(2)))
		// Status Flags
		p.StatusFlags = flag.StatusFlag(FixedLengthInteger.Get(buf.Next(2)))
	}

	return &p, nil
}

func (eof *EOF) Dump(capabilities flag.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer

	payload.WriteByte(eof.EOFHeader)
	if capabilities&flag.ClientProtocol41 != 0 {
		payload.Write(FixedLengthInteger.Dump(uint64(eof.WarningCount), 2))
		payload.Write(FixedLengthInteger.Dump(uint64(eof.StatusFlags), 2))
	}

	eof.Length = uint32(payload.Len())

	dump := make([]byte, 3+1+eof.Length)
	headerDump, err := eof.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	copy(dump, headerDump)
	copy(dump[4:], payload.Bytes())

	return dump, nil
}
