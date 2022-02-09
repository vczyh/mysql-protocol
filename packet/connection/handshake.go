package connection

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
)

// Handshake https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
type Handshake struct {
	generic.Header

	ProtocolVersion         uint8
	ServerVersion           string
	ConnectionId            uint32
	Salt1                   []byte
	CapabilityFlags         generic.CapabilityFlag
	CharacterSet            *generic.Collation
	StatusFlags             generic.StatusFlag
	ExtendedCapabilityFlags generic.CapabilityFlag
	AuthPluginDataLen       uint8
	Salt2                   []byte
	AuthPlugin              generic.AuthenticationPlugin
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
		return nil, generic.ErrPacketData
	}
	p.ProtocolVersion = buf.Next(1)[0]

	// Server Version
	b, err := types.NulTerminatedString.Get(buf)
	if err != nil {
		return nil, err
	}
	p.ServerVersion = string(b)

	// Connection ID
	p.ConnectionId = uint32(types.FixedLengthInteger.Get(buf.Next(4)))

	// Auth Plugin Name Part1
	p.Salt1 = buf.Next(8)

	// Filler
	buf.Next(1)

	// Capability Flags
	p.CapabilityFlags = generic.CapabilityFlag(types.FixedLengthInteger.Get(buf.Next(2)))

	if buf.Len() == 0 {
		return &p, err
	}

	// Character Set
	if buf.Len() == 0 {
		return nil, generic.ErrPacketData
	}
	collationId := buf.Next(1)[0]
	collation, ok := generic.CollationIds[collationId]
	if !ok {
		return nil, fmt.Errorf("unknown collation id %d", collationId)
	}
	p.CharacterSet = collation

	// Status Flags
	p.StatusFlags = generic.StatusFlag(types.FixedLengthInteger.Get(buf.Next(2)))

	// ExtendedCapabilityFlags
	p.ExtendedCapabilityFlags = generic.CapabilityFlag(types.FixedLengthInteger.Get(buf.Next(2)))

	var capabilitiesBs = make([]byte, 4)
	binary.LittleEndian.PutUint16(capabilitiesBs, uint16(p.CapabilityFlags))
	binary.LittleEndian.PutUint16(capabilitiesBs[2:], uint16(p.ExtendedCapabilityFlags))
	capabilities := binary.LittleEndian.Uint32(capabilitiesBs)

	if capabilities&generic.ClientPluginAuth != 0 {
		// Length of auth-plugin-data
		if buf.Len() == 0 {
			return nil, generic.ErrPacketData
		}
		p.AuthPluginDataLen = buf.Next(1)[0]
	} else {
		// 0x00
		buf.Next(1)
	}

	// Reserved
	buf.Next(10)

	// Auth Plugin Name Part2
	if capabilities&generic.ClientSecureConnection != 0 {
		l := 13
		if p.AuthPluginDataLen-8 > 13 {
			l = int(p.AuthPluginDataLen - 8)
		}
		p.Salt2 = buf.Next(l)
	}

	// Auth Plugin Name
	if capabilities&generic.ClientPluginAuth != 0 {
		pluginName, err := types.NulTerminatedString.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AuthPlugin, err = generic.ParseAuthenticationPlugin(string(pluginName)); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func (p *Handshake) GetCapabilities() generic.CapabilityFlag {
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

func (p *Handshake) SetCapabilities(capabilities generic.CapabilityFlag) {
	p.CapabilityFlags = capabilities & 0x0000ffff
	p.ExtendedCapabilityFlags = capabilities & 0xffff0000
}

func (p *Handshake) Dump(capabilities generic.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	// Protocol Version
	payload.WriteByte(p.ProtocolVersion)

	// Server Version
	payload.Write(types.NulTerminatedString.Dump([]byte(p.ServerVersion)))

	// Connection ID
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.ConnectionId), 4))

	// Auth Plugin Name Part1
	payload.Write(p.Salt1)

	// Filler
	payload.WriteByte(0x00)

	// Capability Flags
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.CapabilityFlags), 2))

	// Character Set
	payload.WriteByte(p.CharacterSet.Id)

	// Status Flags
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.StatusFlags), 2))

	// ExtendedCapabilityFlags
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.ExtendedCapabilityFlags>>16), 2))

	// Length of auth-plugin-data
	if capabilities&generic.ClientPluginAuth != 0 {
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
	if capabilities&generic.ClientSecureConnection != 0 {
		payload.Write(p.Salt2)
	}

	// Auth Plugin Name
	if capabilities&generic.ClientPluginAuth != 0 {
		payload.Write(types.NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
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
	generic.Header

	ClientCapabilityFlags generic.CapabilityFlag
	MaxPacketSize         uint32
	CharacterSet          *generic.Collation
	Username              []byte // interpreted by CharacterSet
	AuthRes               []byte
	Database              []byte // interpreted by CharacterSet
	AuthPlugin            generic.AuthenticationPlugin

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
	p.ClientCapabilityFlags = generic.CapabilityFlag(uint32(types.FixedLengthInteger.Get(buf.Next(4))))

	// Max Packet Size
	p.MaxPacketSize = uint32(types.FixedLengthInteger.Get(buf.Next(4)))

	// Character Set
	if buf.Len() == 0 {
		return nil, generic.ErrPacketData
	}
	collationId := buf.Next(1)[0]
	collation, ok := generic.CollationIds[collationId]
	if !ok {
		return nil, fmt.Errorf("unknown collation id %d", collationId)
	}
	p.CharacterSet = collation

	// Reserved
	buf.Next(23)

	// Username
	if p.Username, err = types.NulTerminatedString.Get(buf); err != nil {
		return nil, err
	}

	// Password
	if p.ClientCapabilityFlags&generic.ClientPluginAuthLenencClientData != 0 {
		l, err := types.LengthEncodedInteger.Get(buf)
		if err != nil {
			return nil, err
		}
		p.AuthRes = buf.Next(int(l))
	} else if p.ClientCapabilityFlags&generic.ClientSecureConnection != 0 {
		if buf.Len() == 0 {
			return nil, generic.ErrPacketData
		}
		l := buf.Next(1)[0]
		p.AuthRes = buf.Next(int(l))
	} else {
		if p.AuthRes, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Database
	if p.ClientCapabilityFlags&generic.ClientConnectWithDB != 0 {
		if p.Database, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Auth Plugin Name
	if p.ClientCapabilityFlags&generic.ClientPluginAuth != 0 {
		pluginName, err := types.NulTerminatedString.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AuthPlugin, err = generic.ParseAuthenticationPlugin(string(pluginName)); err != nil {
			return nil, err
		}
	}

	// Attributes
	if p.ClientCapabilityFlags&generic.ClientConnectAttrs != 0 {
		p.AttributeLen, err = types.LengthEncodedInteger.Get(buf)
		if err != nil {
			return nil, err
		}
		if p.AttributeLen > 0 {
			before := buf.Len()
			for before-buf.Len() < int(p.AttributeLen) {
				key, err := types.LengthEncodedString.Get(buf)
				if err != nil {
					return nil, err
				}
				val, err := types.LengthEncodedString.Get(buf)
				if err != nil {
					return nil, err
				}
				p.Attributes = append(p.Attributes, Attribute{string(key), string(val)})
			}
		}
	}

	return &p, nil
}

func (p *HandshakeResponse) Dump(capabilities generic.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer
	// Max Packet Size
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.MaxPacketSize), 4))

	// Character Set
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.CharacterSet.Id), 1))

	// Reserved
	for i := 0; i < 23; i++ {
		payload.WriteByte(0x00)
	}

	// Username
	payload.Write(types.NulTerminatedString.Dump(p.Username))

	// Password
	authResLen := len(p.AuthRes)
	authResLenEncoded := types.LengthEncodedInteger.Dump(uint64(authResLen))
	if len(authResLenEncoded) > 1 {
		p.ClientCapabilityFlags |= generic.ClientPluginAuthLenencClientData
	}
	payload.Write(authResLenEncoded)
	payload.Write(p.AuthRes)

	// Database
	if p.ClientCapabilityFlags&generic.ClientConnectWithDB != 0 {
		payload.Write(types.NulTerminatedString.Dump(p.Database))
	}

	// Auth Plugin Name
	if p.ClientCapabilityFlags&generic.ClientPluginAuth != 0 {
		payload.Write(types.NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
	}

	// Attributes
	if p.ClientCapabilityFlags&generic.ClientConnectAttrs != 0 {
		payload.Write(types.LengthEncodedInteger.Dump(p.AttributeLen))
		for _, attribute := range p.Attributes {
			payload.Write(types.LengthEncodedString.Dump([]byte(attribute.Key)))
			payload.Write(types.LengthEncodedString.Dump([]byte(attribute.Val)))
		}
	}

	// Client Capability Flags
	clientCapabilities := types.FixedLengthInteger.Dump(uint64(p.ClientCapabilityFlags), 4)
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
	p.AttributeLen += uint64(len(types.LengthEncodedString.Dump([]byte(key))))
	p.AttributeLen += uint64(len(types.LengthEncodedString.Dump([]byte(val))))
}

func (p *HandshakeResponse) GetUsername() string {
	return string(p.Username)
}
