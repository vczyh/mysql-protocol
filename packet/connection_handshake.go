package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
)

// Handshake https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
type Handshake struct {
	Header

	ProtocolVersion         uint8
	ServerVersion           string
	ConnectionId            uint32
	Salt1                   []byte
	CapabilityFlags         flag.CapabilityFlag
	CharacterSet            *charset.Collation
	StatusFlags             flag.StatusFlag
	ExtendedCapabilityFlags flag.CapabilityFlag
	AuthPluginDataLen       uint8
	Salt2                   []byte
	AuthPlugin              auth.AuthenticationMethod
}

func ParseHandshake(bs []byte) (*Handshake, error) {
	var p Handshake
	var err error

	buf := bytes.NewBuffer(bs)
	// Header
	if err := p.Parse(buf); err != nil {
		return nil, err
	}

	// Protocol Version
	if buf.Len() == 0 {
		return nil, ErrPacketData
	}
	p.ProtocolVersion = buf.Next(1)[0]

	// Server Version
	b, err := NulTerminatedString.Get(buf)
	if err != nil {
		return nil, err
	}
	p.ServerVersion = string(b)

	// Connection ID
	p.ConnectionId = uint32(FixedLengthInteger.Get(buf.Next(4)))

	// Auth Plugin Name Part1
	p.Salt1 = buf.Next(8)

	// Filler
	buf.Next(1)

	// Capability Flags
	p.CapabilityFlags = flag.CapabilityFlag(FixedLengthInteger.Get(buf.Next(2)))

	if buf.Len() == 0 {
		return &p, err
	}

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

	// Status Flags
	p.StatusFlags = flag.StatusFlag(FixedLengthInteger.Get(buf.Next(2)))

	// ExtendedCapabilityFlags
	p.ExtendedCapabilityFlags = flag.CapabilityFlag(FixedLengthInteger.Get(buf.Next(2)))

	var capabilitiesBs = make([]byte, 4)
	binary.LittleEndian.PutUint16(capabilitiesBs, uint16(p.CapabilityFlags))
	binary.LittleEndian.PutUint16(capabilitiesBs[2:], uint16(p.ExtendedCapabilityFlags))
	capabilities := flag.CapabilityFlag(binary.LittleEndian.Uint32(capabilitiesBs))

	if capabilities&flag.ClientPluginAuth != 0 {
		// Length of auth-plugin-data
		if buf.Len() == 0 {
			return nil, ErrPacketData
		}
		p.AuthPluginDataLen = buf.Next(1)[0]
	} else {
		// 0x00
		buf.Next(1)
	}

	// Reserved
	buf.Next(10)

	// Auth Plugin Name Part2
	if capabilities&flag.ClientSecureConnection != 0 {
		l := 13
		if p.AuthPluginDataLen-8 > 13 {
			l = int(p.AuthPluginDataLen - 8)
		}
		p.Salt2 = buf.Next(l)
	}

	// Auth Plugin Name
	if capabilities&flag.ClientPluginAuth != 0 {
		pluginName, err := NulTerminatedString.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AuthPlugin, err = auth.ParseAuthenticationPlugin(string(pluginName)); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func (p *Handshake) GetCapabilities() flag.CapabilityFlag {
	return p.CapabilityFlags | p.ExtendedCapabilityFlags
}

func (p *Handshake) GetAuthData() []byte {
	salt1 := p.Salt1
	salt2 := p.Salt2
	salt := make([]byte, len(salt1)+len(salt2)-1)
	copy(salt, salt1)
	copy(salt[len(salt1):], salt2[:len(salt2)-1])
	return salt
}

func (p *Handshake) SetCapabilities(capabilities flag.CapabilityFlag) {
	p.CapabilityFlags = capabilities & 0x0000ffff
	p.ExtendedCapabilityFlags = capabilities & 0xffff0000
}

func (p *Handshake) Dump(capabilities flag.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	// Protocol Version
	payload.WriteByte(p.ProtocolVersion)

	// Server Version
	payload.Write(NulTerminatedString.Dump([]byte(p.ServerVersion)))

	// Connection ID
	payload.Write(FixedLengthInteger.Dump(uint64(p.ConnectionId), 4))

	// Auth Plugin Name Part1
	payload.Write(p.Salt1)

	// Filler
	payload.WriteByte(0x00)

	// Capability Flags
	payload.Write(FixedLengthInteger.Dump(uint64(p.CapabilityFlags), 2))

	// Character Set
	payload.WriteByte(p.CharacterSet.Id)

	// Status Flags
	payload.Write(FixedLengthInteger.Dump(uint64(p.StatusFlags), 2))

	// ExtendedCapabilityFlags
	payload.Write(FixedLengthInteger.Dump(uint64(p.ExtendedCapabilityFlags>>16), 2))

	// Length of auth-plugin-data
	if capabilities&flag.ClientPluginAuth != 0 {
		p.AuthPluginDataLen = uint8(len(p.Salt2) + 8)
	} else {
		p.AuthPluginDataLen = 0x00
	}
	payload.WriteByte(p.AuthPluginDataLen)

	// Reserved
	for i := 0; i < 10; i++ {
		payload.WriteByte(0x00)
	}

	// Auth Plugin Name Part2
	if capabilities&flag.ClientSecureConnection != 0 {
		payload.Write(p.Salt2)
	}

	// Auth Plugin Name
	if capabilities&flag.ClientPluginAuth != 0 {
		payload.Write(NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
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

// HandshakeResponse https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
type HandshakeResponse struct {
	Header

	ClientCapabilityFlags flag.CapabilityFlag
	MaxPacketSize         uint32
	CharacterSet          *charset.Collation
	Username              []byte // interpreted by CharacterSet
	AuthRes               []byte
	Database              []byte // interpreted by CharacterSet
	AuthPlugin            auth.AuthenticationMethod

	AttributeLen uint64
	Attributes   []Attribute
}

type Attribute struct {
	Key string
	Val string
}

func ParseHandshakeResponse(bs []byte) (*HandshakeResponse, error) {
	var p HandshakeResponse
	var err error

	buf := bytes.NewBuffer(bs)
	// Header
	if err = p.Parse(buf); err != nil {
		return nil, err
	}

	// Client Capability Flags
	p.ClientCapabilityFlags = flag.CapabilityFlag(uint32(FixedLengthInteger.Get(buf.Next(4))))

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

	// Username
	if p.Username, err = NulTerminatedString.Get(buf); err != nil {
		return nil, err
	}

	// Password
	if p.ClientCapabilityFlags&flag.ClientPluginAuthLenencClientData != 0 {
		l, err := LengthEncodedInteger.Get(buf)
		if err != nil {
			return nil, err
		}
		p.AuthRes = buf.Next(int(l))
	} else if p.ClientCapabilityFlags&flag.ClientSecureConnection != 0 {
		if buf.Len() == 0 {
			return nil, ErrPacketData
		}
		l := buf.Next(1)[0]
		p.AuthRes = buf.Next(int(l))
	} else {
		if p.AuthRes, err = NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Database
	if p.ClientCapabilityFlags&flag.ClientConnectWithDB != 0 {
		if p.Database, err = NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Auth Plugin Name
	if p.ClientCapabilityFlags&flag.ClientPluginAuth != 0 {
		pluginName, err := NulTerminatedString.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AuthPlugin, err = auth.ParseAuthenticationPlugin(string(pluginName)); err != nil {
			return nil, err
		}
	}

	// Attributes
	if p.ClientCapabilityFlags&flag.ClientConnectAttrs != 0 {
		p.AttributeLen, err = LengthEncodedInteger.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AttributeLen > 0 {
			before := buf.Len()
			for before-buf.Len() < int(p.AttributeLen) {
				key, err := LengthEncodedString.Get(buf)
				if err != nil {
					return nil, err
				}
				val, err := LengthEncodedString.Get(buf)
				if err != nil {
					return nil, err
				}
				p.Attributes = append(p.Attributes, Attribute{string(key), string(val)})
			}
		}
	}

	return &p, nil
}

func (p *HandshakeResponse) Dump(capabilities flag.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	// Max Packet Size
	payload.Write(FixedLengthInteger.Dump(uint64(p.MaxPacketSize), 4))

	// Character Set
	payload.Write(FixedLengthInteger.Dump(uint64(p.CharacterSet.Id), 1))

	// Reserved
	for i := 0; i < 23; i++ {
		payload.WriteByte(0x00)
	}

	// Username
	payload.Write(NulTerminatedString.Dump(p.Username))

	// Password
	authResLen := len(p.AuthRes)
	authResLenEncoded := LengthEncodedInteger.Dump(uint64(authResLen))
	if len(authResLenEncoded) > 1 {
		p.ClientCapabilityFlags |= flag.ClientPluginAuthLenencClientData
	}
	payload.Write(authResLenEncoded)
	payload.Write(p.AuthRes)

	// Database
	if p.ClientCapabilityFlags&flag.ClientConnectWithDB != 0 {
		payload.Write(NulTerminatedString.Dump(p.Database))
	}

	// Auth Plugin Name
	if p.ClientCapabilityFlags&flag.ClientPluginAuth != 0 {
		payload.Write(NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
	}

	// Attributes
	if p.ClientCapabilityFlags&flag.ClientConnectAttrs != 0 {
		payload.Write(LengthEncodedInteger.Dump(p.AttributeLen))
		for _, attribute := range p.Attributes {
			payload.Write(LengthEncodedString.Dump([]byte(attribute.Key)))
			payload.Write(LengthEncodedString.Dump([]byte(attribute.Val)))
		}
	}

	// Client Capability Flags
	clientCapabilities := FixedLengthInteger.Dump(uint64(p.ClientCapabilityFlags), 4)
	payloadBs := append(clientCapabilities, payload.Bytes()...)

	p.Length = uint32(len(payloadBs))

	dump := make([]byte, 3+1+p.Length)
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	copy(dump, headerDump)
	copy(dump[4:], payloadBs)

	return dump, nil
}

func (p *HandshakeResponse) AddAttribute(key string, val string) {
	p.Attributes = append(p.Attributes, Attribute{key, val})
	p.AttributeLen += uint64(len(LengthEncodedString.Dump([]byte(key))))
	p.AttributeLen += uint64(len(LengthEncodedString.Dump([]byte(val))))
}

func (p *HandshakeResponse) GetUsername() string {
	return string(p.Username)
}
