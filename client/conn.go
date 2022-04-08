package client

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"net"
	"time"
)

const (
	maxPacketSize = 1<<24 - 1
)

type Conn struct {
	host      string
	port      int
	user      string
	password  string
	loc       *time.Location
	attrs     map[string]string
	collation *charset.Collation

	useSSL             bool
	insecureSkipVerify bool
	sslCA              string
	sslCert            string
	sslKey             string

	mysqlConn mysql.Conn

	status       flag.Status
	affectedRows uint64
	lastInsertId uint64
}

func CreateConnection(opts ...Option) (*Conn, error) {
	c := new(Conn)
	for _, opt := range opts {
		opt.apply(c)
	}

	if err := c.build(); err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", c.host, c.port))
	if err != nil {
		return nil, err
	}

	c.mysqlConn = mysql.NewClientConnection(conn, c.defaultCapabilities())
	return c, c.dial()
}

func (c *Conn) Capabilities() flag.Capability {
	return c.mysqlConn.Capabilities()
}

func (c *Conn) AffectedRows() uint64 {
	return c.affectedRows
}

func (c *Conn) LastInsertId() uint64 {
	return c.lastInsertId
}

func (c *Conn) ReadPacket() ([]byte, error) {
	return c.mysqlConn.ReadPacket()
}

func (c *Conn) WritePacket(pkt packet.Packet) error {
	return c.mysqlConn.WritePacket(pkt)
}

func (c *Conn) WriteCommandPacket(pkt packet.Packet) error {
	return c.mysqlConn.WriteCommandPacket(pkt)
}

func (c *Conn) Ping() error {
	if err := c.WriteCommandPacket(packet.NewCmd(packet.ComPing, nil)); err != nil {
		return err
	}
	return c.readOKERRPacket()
}

func (c *Conn) Close() error {
	c.quit()
	return c.mysqlConn.Close()
}

func (c *Conn) build() error {
	if c.loc == nil {
		c.loc = time.Local
	}
	if c.collation == nil {
		collation, err := charset.GetCollationByName(charset.UTF8MB4GeneralCi)
		if err != nil {
			return err
		}
		c.collation = collation
	}
	return nil
}

func (c *Conn) quit() error {
	if err := c.WriteCommandPacket(packet.NewCmd(packet.ComQuit, nil)); err != nil {
		return err
	}

	data, err := c.ReadPacket()
	// response is either a connection close or a OK_Packet
	if err == nil && packet.IsOK(data) {
		return nil
	}
	return nil
}

func (c *Conn) dial() error {
	hs, err := c.handleHandshakePacket()
	if err != nil {
		return err
	}

	if err := c.handleSSL(); err != nil {
		return err
	}

	method := hs.AuthPlugin
	authData := hs.GetAuthData()
	if err := c.writeHandshakeResponsePacket(method, authData); err != nil {
		return err
	}

	return c.auth(method, authData)
}

func (c *Conn) handleHandshakePacket() (*packet.Handshake, error) {
	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	if packet.IsErr(data) {
		return nil, c.handleOKERRPacket(data)
	}

	pkt, err := packet.ParseHandshake(data)
	if err != nil {
		return nil, err
	}

	if pkt.GetCapabilities()&flag.ClientSSL != 0 && c.useSSL {
		c.mysqlConn.SetCapabilities(c.Capabilities() | flag.ClientSSL)
	}
	return pkt, nil
}

func (c *Conn) writeHandshakeResponsePacket(method auth.Method, authData []byte) error {
	authRes, err := c.generateAuthRes(method, authData)
	if err != nil {
		return err
	}

	pkt := &packet.HandshakeResponse{
		ClientCapabilityFlags: c.Capabilities(),
		MaxPacketSize:         maxPacketSize,
		CharacterSet:          c.collation,
		Username:              []byte(c.user),
		AuthRes:               authRes,
		AuthPlugin:            method,
	}

	if len(c.attrs) > 0 {
		pkt.ClientCapabilityFlags |= flag.ClientConnectAttrs
		for key, val := range c.attrs {
			pkt.AddAttribute(key, val)
		}
	}

	c.mysqlConn.SetCapabilities(pkt.ClientCapabilityFlags)
	return c.WritePacket(pkt)
}

func (c *Conn) defaultCapabilities() flag.Capability {
	return flag.ClientProtocol41 |
		flag.ClientSecureConnection |
		flag.ClientPluginAuth |
		flag.ClientLongPassword |
		flag.ClientLongFlag |
		flag.ClientTransactions |
		flag.ClientInteractive |
		flag.ClientMultiResults
}

func (c *Conn) readUntilEOFPacket() error {
	for {
		data, err := c.ReadPacket()
		if err != nil {
			return err
		}

		switch {
		case packet.IsErr(data):
			return c.handleOKERRPacket(data)
		case packet.IsEOF(data):
			eofPkt, err := packet.ParseEOF(data, c.mysqlConn.Capabilities())
			if err != nil {
				return err
			}
			c.status = eofPkt.StatusFlags
			return nil
		}
	}
}

func (c *Conn) handleOKERRPacket(data []byte) error {
	switch {
	case packet.IsOK(data):
		okPkt, err := packet.ParseOk(data, c.mysqlConn.Capabilities())
		if err != nil {
			return err
		}
		c.affectedRows = okPkt.AffectedRows
		c.lastInsertId = okPkt.LastInsertId
		c.status = okPkt.StatusFlags
		return nil

	case packet.IsErr(data):
		errPkt, err := packet.ParseERR(data, c.mysqlConn.Capabilities())
		if err != nil {
			return err
		}
		// TODO convert to mysql error
		return errPkt

	default:
		return packet.ErrPacketData
	}
}

func (c *Conn) readOKERRPacket() error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}
	return c.handleOKERRPacket(data)
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

func WithLocation(loc *time.Location) Option {
	return optionFun(func(c *Conn) {
		c.loc = loc
	})
}

func WithAttribute(key string, val string) Option {
	return optionFun(func(c *Conn) {
		if c.attrs == nil {
			c.attrs = make(map[string]string)
			c.attrs[key] = val
		}
	})
}

func WithCollation(collation *charset.Collation) Option {
	return optionFun(func(c *Conn) {
		c.collation = collation
	})
}

func WithUseSSL(useSSL bool) Option {
	return optionFun(func(c *Conn) {
		c.useSSL = useSSL
	})
}

func WithSSLCA(sslCA string) Option {
	return optionFun(func(c *Conn) {
		c.sslCA = sslCA
	})
}

func WithInsecureSkipVerify(insecureSkipVerify bool) Option {
	return optionFun(func(c *Conn) {
		c.insecureSkipVerify = insecureSkipVerify
	})
}

func WithSSLCert(sslCert string) Option {
	return optionFun(func(c *Conn) {
		c.sslCert = sslCert
	})
}

func WithSSLKey(sslKey string) Option {
	return optionFun(func(c *Conn) {
		c.sslKey = sslKey
	})
}

type Option interface {
	apply(*Conn)
}

type optionFun func(*Conn)

func (f optionFun) apply(c *Conn) {
	f(c)
}
