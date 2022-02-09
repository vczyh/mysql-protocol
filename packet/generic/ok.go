package generic

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/packet/types"
)

// OK https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
type OK struct {
	Header

	OKHeader            uint8
	AffectedRows        uint64
	LastInsertId        uint64
	StatusFlags         StatusFlag
	WarningCount        uint16
	Info                []byte
	SessionStateChanges []byte // todo
}

const (
	SESSION_TRACK_SYSTEM_VARIABLES = 0x00
	SESSION_TRACK_SCHEMA           = 0x01
	SESSION_TRACK_STATE_CHANGE     = 0x02
	SESSION_TRACK_GTIDS            = 0x03
)

//type SessionState struct {
//	Type uint8
//	Data []byte
//}

func ParseOk(bs []byte, capabilities CapabilityFlag) (*OK, error) {
	var p OK
	var err error

	buf := bytes.NewBuffer(bs)
	// Header
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	// OK Header
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.OKHeader = buf.Next(1)[0]

	// Affected Rows
	if p.AffectedRows, err = types.LengthEncodedInteger.Get(buf); err != nil {
		return nil, err
	}

	// Last Insert Id
	if p.LastInsertId, err = types.LengthEncodedInteger.Get(buf); err != nil {
		return nil, err
	}

	// Status Flags
	if capabilities&ClientProtocol41 != 0 {
		p.StatusFlags = StatusFlag(uint16(types.FixedLengthInteger.Get(buf.Next(2))))
		p.WarningCount = uint16(types.FixedLengthInteger.Get(buf.Next(2)))
	} else if capabilities&ClientTransactions != 0 {
		p.StatusFlags = StatusFlag(uint16(types.FixedLengthInteger.Get(buf.Next(2))))
	}

	if capabilities&ClientSessionTrack != 0 {
		// Info
		if p.Info, err = types.LengthEncodedString.Get(buf); err != nil {
			return nil, err
		}

		// todo
		// Session State Changes
		if p.StatusFlags&ServerSessionStateChanged != 0 {
			if p.SessionStateChanges, err = types.LengthEncodedString.Get(buf); err != nil {
				return nil, err
			}
		}
	} else {
		// Info
		p.Info = buf.Bytes()
	}

	return &p, nil
}

func (p *OK) Dump(capabilities CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(p.OKHeader)
	payload.Write(types.LengthEncodedInteger.Dump(p.AffectedRows))
	payload.Write(types.LengthEncodedInteger.Dump(p.LastInsertId))

	if capabilities&ClientProtocol41 != 0 {
		payload.Write(types.FixedLengthInteger.Dump(uint64(p.StatusFlags), 2))
		payload.Write(types.FixedLengthInteger.Dump(uint64(p.WarningCount), 2))
	} else if capabilities&ClientTransactions != 0 {
		payload.Write(types.FixedLengthInteger.Dump(uint64(p.StatusFlags), 2))
	}

	if capabilities&ClientSessionTrack != 0 {
		// Info
		payload.Write(types.LengthEncodedString.Dump(p.Info))

		// todo
		// Session State Changes
		if p.StatusFlags&ServerSessionStateChanged != 0 {
			payload.Write(types.LengthEncodedString.Dump(p.SessionStateChanges))
		}
	} else {
		// Info
		payload.Write(p.Info)
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
