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

type Conn interface {
	Capabilities() flag.Capability
	AffectedRows() uint64
	LastInsertId() uint64

	ReadPacket() ([]byte, error)

	WritePacket(packet.Packet) error
	WriteCommandPacket(packet.Packet) error

	Exec(string) (mysql.Result, error)
	Query(string) (*Rows, error)

	Ping() error
	Close() error
}

const (
	maxPacketSize = 1<<24 - 1
)

type conn struct {
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

func CreateConnection(opts ...Option) (Conn, error) {
	c := new(conn)
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

func (c *conn) Capabilities() flag.Capability {
	return c.mysqlConn.Capabilities()
}

func (c *conn) AffectedRows() uint64 {
	return c.affectedRows
}

func (c *conn) LastInsertId() uint64 {
	return c.lastInsertId
}

func (c *conn) ReadPacket() ([]byte, error) {
	return c.mysqlConn.ReadPacket()
}

func (c *conn) WritePacket(pkt packet.Packet) error {
	return c.mysqlConn.WritePacket(pkt)
}

func (c *conn) WriteCommandPacket(pkt packet.Packet) error {
	return c.mysqlConn.WriteCommandPacket(pkt)
}

func (c *conn) Ping() error {
	if err := c.WriteCommandPacket(packet.NewCmd(packet.ComPing, nil)); err != nil {
		return err
	}
	return c.readOKERRPacket()
}

func (c *conn) Close() error {
	c.quit()
	return c.mysqlConn.Close()
}

func (c *conn) build() error {
	if c.loc == nil {
		c.loc = time.Local
	}
	if c.collation == nil {
		c.collation = charset.Utf8mb4GeneralCi
	}
	return nil
}

func (c *conn) quit() error {
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

func (c *conn) dial() error {
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

func (c *conn) handleHandshakePacket() (*packet.Handshake, error) {
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

func (c *conn) writeHandshakeResponsePacket(method auth.Method, authData []byte) error {
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

func (c *conn) defaultCapabilities() flag.Capability {
	return flag.ClientProtocol41 |
		flag.ClientSecureConnection |
		flag.ClientPluginAuth |
		flag.ClientLongPassword |
		flag.ClientLongFlag |
		flag.ClientTransactions |
		flag.ClientInteractive |
		flag.ClientMultiResults
}

func (c *conn) readUntilEOFPacket() error {
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

func (c *conn) handleOKERRPacket(data []byte) error {
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

func (c *conn) readOKERRPacket() error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}
	return c.handleOKERRPacket(data)
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

func WithLocation(loc *time.Location) Option {
	return optionFun(func(c *conn) {
		c.loc = loc
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

func WithCollation(collation *charset.Collation) Option {
	return optionFun(func(c *conn) {
		c.collation = collation
	})
}

func WithUseSSL(useSSL bool) Option {
	return optionFun(func(c *conn) {
		c.useSSL = useSSL
	})
}

func WithSSLCA(sslCA string) Option {
	return optionFun(func(c *conn) {
		c.sslCA = sslCA
	})
}

func WithInsecureSkipVerify(insecureSkipVerify bool) Option {
	return optionFun(func(c *conn) {
		c.insecureSkipVerify = insecureSkipVerify
	})
}

func WithSSLCert(sslCert string) Option {
	return optionFun(func(c *conn) {
		c.sslCert = sslCert
	})
}

func WithSSLKey(sslKey string) Option {
	return optionFun(func(c *conn) {
		c.sslKey = sslKey
	})
}

type Option interface {
	apply(*conn)
}

type optionFun func(*conn)

func (f optionFun) apply(c *conn) {
	f(c)
}
