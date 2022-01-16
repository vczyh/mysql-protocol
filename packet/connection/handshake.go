package connection

import (
	"bytes"
	"encoding/binary"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

// Handshake https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
type Handshake struct {
	generic.Header

	ProtocolVersion         uint8
	ServerVersion           []byte
	ConnectionId            uint32
	Salt1                   []byte
	CapabilityFlags         uint16
	CharacterSet            uint8
	StatusFlags             uint16
	ExtendedCapabilityFlags uint16
	AuthPluginDataLen       uint8
	Salt2                   []byte
	AuthPluginName          []byte
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
	if p.ServerVersion, err = types.NulTerminatedString.Get(buf); err != nil {
		return nil, err
	}

	// Connection ID
	p.ConnectionId = uint32(types.FixedLengthInteger.Get(buf.Next(4)))

	// Auth Plugin Name Part1
	p.Salt1 = buf.Next(8)

	// Filler
	buf.Next(1)

	// Capability Flags
	p.CapabilityFlags = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	if buf.Len() == 0 {
		return &p, err
	}

	// Character Set
	if buf.Len() == 0 {
		return nil, generic.ErrPacketData
	}
	p.CharacterSet = buf.Next(1)[0]

	// Status Flags
	p.StatusFlags = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	// ExtendedCapabilityFlags
	p.ExtendedCapabilityFlags = uint16(types.FixedLengthInteger.Get(buf.Next(2)))

	var capabilitiesBs = make([]byte, 4)
	binary.LittleEndian.PutUint16(capabilitiesBs, p.CapabilityFlags)
	binary.LittleEndian.PutUint16(capabilitiesBs[2:], p.ExtendedCapabilityFlags)
	capabilities := binary.LittleEndian.Uint32(capabilitiesBs)

	if capabilities&generic.CLIENT_PLUGIN_AUTH != 0x00000000 {
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
	if capabilities&generic.CLIENT_SECURE_CONNECTION != 0x00000000 {
		l := 13
		if p.AuthPluginDataLen-8 > 13 {
			l = int(p.AuthPluginDataLen - 8)
		}
		p.Salt2 = buf.Next(l)
	}

	// Auth Plugin Name
	if capabilities&generic.CLIENT_PLUGIN_AUTH != 0x00000000 {
		if p.AuthPluginName, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func (p *Handshake) GetCharacterSet() *Collation {
	collation, ok := CollationIds[p.CharacterSet]
	if !ok {
		return Utf8mb4GeneralCi
	}
	return collation
}

func (p *Handshake) GetAuthData() []byte {
	salt1 := p.Salt1
	salt2 := p.Salt2
	salt := make([]byte, len(salt1)+len(salt2)-1)
	copy(salt, salt1)
	copy(salt[len(salt1):], salt2[:len(salt2)-1])
	return salt
}

func (p *Handshake) GetPlugin() AuthenticationPlugin {
	switch string(p.AuthPluginName) {
	case MySQLNativePassword.String():
		return MySQLNativePassword
	case CachingSHA2Password.String():
		return CachingSHA2Password
	default:
		return MySQLNativePassword
	}
}

//func (p *Handshake) String() string {
//	var sb strings.Builder
//	sb.WriteString("Header: { ")
//	sb.WriteString("Length: " + strconv.Itoa(int(p.Header.Length)) + " ")
//	sb.WriteString("Sequence: " + strconv.Itoa(int(p.Header.Seq)) + " ")
//	sb.WriteString("}")
//	sb.WriteString("Payload: {")
//	sb.WriteString("ProtocolVersion: " + strconv.Itoa(int(p.ProtocolVersion)))
//	sb.WriteString("ServerVersion: " + string(p.ServerVersion))
//	sb.WriteString("ConnectionId: " + strconv.Itoa(int(p.ConnectionId)))
//	sb.WriteString("Salt1: " + string(p.Salt1))
//	sb.WriteString("CapabilityFlags: " + fmt.Sprintf("%x", p.CapabilityFlags))
//	sb.WriteString("CharacterSet: {" fmt.Sprintf("%v"))
//
//}

// HandshakeResponse https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
type HandshakeResponse struct {
	generic.Header

	ClientCapabilityFlags uint32
	MaxPacketSize         uint32
	CharacterSet          uint8
	Username              []byte
	AuthRes               []byte
	Database              []byte
	AuthPluginName        []byte

	AttributeLen uint64
	Attributes   []*Attribute
}

type Attribute struct {
	Key []byte
	Val []byte
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
	p.ClientCapabilityFlags = uint32(types.FixedLengthInteger.Get(buf.Next(4)))

	// Max Packet Size
	p.MaxPacketSize = uint32(types.FixedLengthInteger.Get(buf.Next(4)))

	// Character Set
	if buf.Len() == 0 {
		return nil, generic.ErrPacketData
	}
	p.CharacterSet = buf.Next(1)[0]

	// Reserved
	buf.Next(23)

	// Username
	if p.Username, err = types.NulTerminatedString.Get(buf); err != nil {
		return nil, err
	}

	// Password
	if p.ClientCapabilityFlags&generic.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0x00000000 {
		l, err := types.LengthEncodedInteger.Get(buf)
		if err != nil {
			return nil, err
		}
		p.AuthRes = buf.Next(int(l))
	} else if p.ClientCapabilityFlags&generic.CLIENT_SECURE_CONNECTION != 0x00000000 {
		p.AuthRes = buf.Next(1)
	} else {
		if p.AuthRes, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Database
	if p.ClientCapabilityFlags&generic.CLIENT_CONNECT_WITH_DB != 0x00000000 {
		if p.Database, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Auth Plugin Name
	if p.ClientCapabilityFlags&generic.CLIENT_PLUGIN_AUTH != 0x00000000 {
		if p.AuthPluginName, err = types.NulTerminatedString.Get(buf); err != nil {
			return nil, err
		}
	}

	// Attributes
	if p.ClientCapabilityFlags&generic.CLIENT_CONNECT_ATTRS != 0x00000000 {
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
				p.Attributes = append(p.Attributes, &Attribute{key, val})
			}
		}
	}

	return &p, nil
}

func (p *HandshakeResponse) Dump() []byte {
	var payload bytes.Buffer
	// Max Packet Size
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.MaxPacketSize), 4))

	// Character Set
	payload.Write(types.FixedLengthInteger.Dump(uint64(p.CharacterSet), 1))

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
		p.ClientCapabilityFlags |= generic.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	}
	payload.Write(authResLenEncoded)
	payload.Write(p.AuthRes)

	// Database
	if len(p.Database) > 0 {
		p.ClientCapabilityFlags |= generic.CLIENT_CONNECT_WITH_DB
		payload.Write(types.NulTerminatedString.Dump(p.Database))
	}

	// Auth Plugin Name
	if len(p.AuthPluginName) > 0 {
		p.ClientCapabilityFlags |= generic.CLIENT_PLUGIN_AUTH
		payload.Write(types.NulTerminatedString.Dump(p.AuthPluginName))
	}

	// Attributes
	if p.AttributeLen > 0 {
		p.ClientCapabilityFlags |= generic.CLIENT_CONNECT_ATTRS
		payload.Write(types.LengthEncodedInteger.Dump(p.AttributeLen))
		for _, attribute := range p.Attributes {
			payload.Write(types.LengthEncodedString.Dump(attribute.Key))
			payload.Write(types.LengthEncodedString.Dump(attribute.Val))
		}
	}

	// Client Capability Flags
	capabilities := types.FixedLengthInteger.Dump(uint64(p.ClientCapabilityFlags), 4)
	payloadBs := append(capabilities, payload.Bytes()...)

	p.Length = uint32(len(payloadBs))

	dump := make([]byte, 3+1+p.Length)
	copy(dump, p.Header.Dump())
	copy(dump[4:], payloadBs)

	return dump
}

func (p *HandshakeResponse) SetCharacterSet(c *Collation) {
	p.CharacterSet = c.Id
}

func (p *HandshakeResponse) SetUsername(username string) {
	p.Username = []byte(username)
}

func (p *HandshakeResponse) SetPassword(plugin AuthenticationPlugin, password string, salt []byte) (err error) {
	p.AuthRes, err = EncryptPassword(plugin, []byte(password), salt)
	return err
}

func (p *HandshakeResponse) SetAuthPlugin(plugin AuthenticationPlugin) {
	p.AuthPluginName = []byte(plugin.String())
}

func (p *HandshakeResponse) AddAttribute(key string, val string) {
	k, v := []byte(key), []byte(val)
	p.Attributes = append(p.Attributes, &Attribute{k, v})
	p.AttributeLen += uint64(len(types.LengthEncodedString.Dump(k)))
	p.AttributeLen += uint64(len(types.LengthEncodedString.Dump(v)))
}

func (p *HandshakeResponse) GetUsername() string {
	return string(p.Username)
}
