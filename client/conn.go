package client

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/connection"
	"mysql-protocol/packet/generic"
	"net"
)

type Conn struct {
	host     string
	port     int
	user     string
	password string

	subConn
	sequence uint8

	Capabilities uint32
	AffectedRows uint64
	LastInsertId uint64
	Status       uint16
}

func CreateConnection(opts ...Option) (*Conn, error) {
	var c Conn
	var err error
	for _, opt := range opts {
		opt.apply(&c)
	}

	c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.host, c.port))
	if err != nil {
		return nil, err
	}

	return &c, c.dial()
}

func (c *Conn) Close() error {
	_ = c.Quit()
	return c.subConn.close()
}

// Quit is implement of the COM_QUIT
func (c *Conn) Quit() error {
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

// Ping is implement of the COM_PING
func (c *Conn) Ping() error {
	pkt := command.NewPing()
	if err := c.WriteCommandPacket(pkt); err != nil {
		return err
	}
	return c.ReadOKErrPacket()
}

func (c *Conn) dial() error {
	handshake, err := c.handshake()
	if err != nil {
		return err
	}

	if err := c.handshakeResponse(handshake); err != nil {
		return err
	}
	return c.auth(handshake.GetPlugin(), handshake.GetAuthData())
}

func (c *Conn) handshake() (*connection.Handshake, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	if generic.IsErr(data) {
		return nil, c.HandleOKErrPacket(data)
	}
	return connection.ParseHandshake(data)
}

func (c *Conn) handshakeResponse(handshake *connection.Handshake) error {
	serverPlugin := handshake.GetPlugin()
	authData := handshake.GetAuthData()

	var p connection.HandshakeResponse
	p.ClientCapabilityFlags |= generic.CLIENT_PROTOCOL_41 |
		generic.CLIENT_SECURE_CONNECTION |
		generic.CLIENT_LONG_PASSWORD |
		generic.CLIENT_LONG_FLAG |
		generic.CLIENT_TRANSACTIONS |
		generic.CLIENT_INTERACTIVE |
		generic.CLIENT_MULTI_RESULTS
	p.MaxPacketSize = 16777215
	p.SetCharacterSet(connection.Utf8GeneralCi)
	p.SetUsername(c.user)
	if err := p.SetPassword(serverPlugin, c.password, authData); err != nil {
		return err
	}
	p.SetAuthPlugin(handshake.GetPlugin())
	p.AddAttribute("_client_name", "pymysql")
	p.AddAttribute("_pid", "41674")
	p.AddAttribute("_client_version", "1.0.2")
	p.AddAttribute("program_name", "mycli")

	c.Capabilities = p.ClientCapabilityFlags
	return c.WritePacket(&p)
}

func (c *Conn) ReadPacket() ([]byte, error) {
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

func (c *Conn) ReadUntilEOFPacket() error {
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
			eofPkt, err := generic.ParseEOF(data, c.Capabilities)
			if err != nil {
				return err
			}
			c.Status = eofPkt.StatusFlags // todo test
			return nil
		}
	}
}

func (c *Conn) WritePacket(packet generic.Packet) error {
	c.sequence++
	packet.SetSequence(int(c.sequence))
	fmt.Println(hex.Dump(packet.Dump())) // todo
	_, err := c.subConn.Write(packet.Dump())
	return err
}

func (c *Conn) WriteCommandPacket(pkt generic.Packet) error {
	c.sequence = 0
	pkt.SetSequence(int(c.sequence))
	fmt.Println(hex.Dump(pkt.Dump())) // todo
	_, err := c.subConn.Write(pkt.Dump())
	return err
}

func (c *Conn) HandleOKErrPacket(data []byte) error {
	switch {
	case generic.IsOK(data):
		okPkt, err := generic.ParseOk(data, c.Capabilities)
		if err != nil {
			return err
		}

		c.AffectedRows = okPkt.AffectedRows
		c.LastInsertId = okPkt.LastInsertId
		c.Status = okPkt.StatusFlags // todo test

		return nil

	case generic.IsErr(data):
		errPkt, err := generic.ParseERR(data, c.Capabilities)
		if err != nil {
			return err
		}
		return errPkt

	default:
		return generic.ErrPacketData
	}
}

func (c *Conn) ReadOKErrPacket() error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}
	fmt.Println(hex.Dump(data)) // TODO
	return c.HandleOKErrPacket(data)
}

func WithHost(host string) Option {
	return optionFun(func(c *Conn) {
		c.host = host
	})
}

func WithPort(port int) Option {
	return optionFun(func(c *Conn) {
		c.port = port
	})
}

func WithUser(user string) Option {
	return optionFun(func(c *Conn) {
		c.user = user
	})
}

func WithPassword(password string) Option {
	return optionFun(func(c *Conn) {
		c.password = password
	})
}

type Option interface {
	apply(*Conn)
}

type optionFun func(*Conn)

func (f optionFun) apply(c *Conn) {
	f(c)
}
