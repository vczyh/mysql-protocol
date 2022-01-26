package client

import (
	"encoding/binary"
	"fmt"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/connection"
	"mysql-protocol/packet/generic"
	"net"
)

type Conn interface {
	Capabilities() generic.CapabilityFlag
	Status() generic.StatusFlag

	AffectedRows() uint64
	LastInsertId() uint64

	ReadPacket() ([]byte, error)
	ReadOKErrPacket() error
	ReadUntilEOFPacket() error

	WritePacket(generic.Packet) error
	WriteCommandPacket(generic.Packet) error

	HandleOKErrPacket([]byte) error

	Ping() error
	Close() error
}

type conn struct {
	host     string
	port     int
	user     string
	password string
	attrs    map[string]string

	subConn
	sequence uint8

	capabilities generic.CapabilityFlag
	status       generic.StatusFlag
	affectedRows uint64
	lastInsertId uint64
}

func CreateConnection(opts ...Option) (Conn, error) {
	var err error
	c := new(conn)

	for _, opt := range opts {
		opt.apply(c)
	}

	c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.host, c.port))
	if err != nil {
		return nil, err
	}

	return c, c.dial()
}

func (c *conn) Close() error {
	_ = c.quit()
	return c.subConn.close()
}

func (c *conn) quit() error {
	pkt := command.NewQuit()
	if err := c.WriteCommandPacket(pkt); err != nil {
		return err
	}

	data, err := c.ReadPacket()
	// response is either a connection close or a OK_Packet
	if err == nil && generic.IsOK(data) {
		return nil
	}
	return err
}

func (c *conn) Ping() error {
	pkt := command.NewPing()
	if err := c.WriteCommandPacket(pkt); err != nil {
		return err
	}
	return c.ReadOKErrPacket()
}

func (c *conn) dial() error {
	handshake, err := c.handshake()
	if err != nil {
		return err
	}

	if err := c.handshakeResponse(handshake); err != nil {
		return err
	}
	return c.auth(handshake.AuthPlugin, handshake.GetAuthData())
}

func (c *conn) handshake() (*connection.Handshake, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	if generic.IsErr(data) {
		return nil, c.HandleOKErrPacket(data)
	}
	return connection.ParseHandshake(data)
}

func (c *conn) handshakeResponse(handshake *connection.Handshake) error {
	serverPlugin := handshake.AuthPlugin
	authData := handshake.GetAuthData()

	var pkt connection.HandshakeResponse

	pkt.ClientCapabilityFlags |= generic.ClientProtocol41 |
		generic.ClientSecureConnection |
		generic.ClientPluginAuth |
		generic.ClientLongPassword |
		generic.ClientLongFlag |
		generic.ClientTransactions |
		generic.ClientInteractive |
		generic.ClientMultiResults

	// TODO max packet size
	pkt.MaxPacketSize = 16777215
	pkt.CharacterSet = generic.Utf8mb4GeneralCi
	pkt.Username = []byte(c.user)
	pkt.AuthPlugin = serverPlugin

	passwordEncrypted, err := generic.EncryptPassword(serverPlugin, []byte(c.password), authData)
	if err != nil {
		return err
	}
	pkt.AuthRes = passwordEncrypted

	if len(c.attrs) > 0 {
		pkt.ClientCapabilityFlags |= generic.ClientConnectAttrs
		for key, val := range c.attrs {
			pkt.AddAttribute(key, val)
		}
	}

	c.capabilities = pkt.ClientCapabilityFlags

	return c.WritePacket(&pkt)
}

func (c *conn) ReadPacket() ([]byte, error) {
	// payload length
	lenData, err := c.Next(3)
	if err != nil {
		return nil, err
	}
	lenData = append(lenData, 0x00)
	length := binary.LittleEndian.Uint32(lenData)

	// sequence
	seqData, err := c.Next(1)
	if err != nil {
		return nil, err
	}
	if len(seqData) == 0 {
		return nil, generic.ErrPacketData
	}
	c.sequence = seqData[0]

	// payload
	payloadData, err := c.Next(int(length))
	if err != nil {
		return nil, err
	}

	// packet bytes
	packetData := append(lenData[:len(lenData)-1], seqData...)
	packetData = append(packetData, payloadData...)

	return packetData, nil
}

func (c *conn) ReadUntilEOFPacket() error {
	for {
		data, err := c.ReadPacket()
		if err != nil {
			return err
		}

		switch {
		case generic.IsErr(data):
			return c.HandleOKErrPacket(data)

		case generic.IsEOF(data):
			// TODO status
			eofPkt, err := generic.ParseEOF(data, c.capabilities)
			if err != nil {
				return err
			}
			c.status = eofPkt.StatusFlags // todo test
			return nil
		}
	}
}

func (c *conn) WritePacket(packet generic.Packet) error {
	c.sequence++
	packet.SetSequence(int(c.sequence))
	_, err := c.subConn.Write(packet.Dump())
	return err
}

func (c *conn) WriteCommandPacket(pkt generic.Packet) error {
	c.sequence = 0
	pkt.SetSequence(int(c.sequence))
	_, err := c.subConn.Write(pkt.Dump())
	return err
}

func (c *conn) HandleOKErrPacket(data []byte) error {
	switch {
	case generic.IsOK(data):
		okPkt, err := generic.ParseOk(data, c.capabilities)
		if err != nil {
			return err
		}

		c.affectedRows = okPkt.AffectedRows
		c.lastInsertId = okPkt.LastInsertId
		c.status = okPkt.StatusFlags // todo test

		return nil

	case generic.IsErr(data):
		errPkt, err := generic.ParseERR(data, c.capabilities)
		if err != nil {
			return err
		}
		return errPkt

	default:
		return generic.ErrPacketData
	}
}

func (c *conn) ReadOKErrPacket() error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}
	return c.HandleOKErrPacket(data)
}

func (c *conn) Capabilities() generic.CapabilityFlag {
	return c.capabilities
}

func (c *conn) Status() generic.StatusFlag {
	return c.status
}

func (c *conn) AffectedRows() uint64 {
	return c.affectedRows
}

func (c *conn) LastInsertId() uint64 {
	return c.lastInsertId
}

func WithHost(host string) Option {
	return optionFun(func(c *conn) {
		c.host = host
	})
}

func WithPort(port int) Option {
	return optionFun(func(c *conn) {
		c.port = port
	})
}

func WithUser(user string) Option {
	return optionFun(func(c *conn) {
		c.user = user
	})
}

func WithPassword(password string) Option {
	return optionFun(func(c *conn) {
		c.password = password
	})
}

func WithAttribute(key string, val string) Option {
	return optionFun(func(c *conn) {
		if c.attrs == nil {
			c.attrs = make(map[string]string)
			c.attrs[key] = val
		}
	})
}

type Option interface {
	apply(*conn)
}

type optionFun func(*conn)

func (f optionFun) apply(c *conn) {
	f(c)
}
