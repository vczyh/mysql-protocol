package mysql

import (
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/errors"
	"github.com/vczyh/mysql-protocol/packet"
	"net"
)

type Conn interface {
	SetCapabilities(capabilities core.CapabilityFlag)

	ClientTLS(config *tls.Config)
	ServerTLS(config *tls.Config)
	TLS() bool

	ConnectionId() uint32
	Capabilities() core.CapabilityFlag

	RemoteAddr() net.Addr

	ReadPacket() ([]byte, error)

	WritePacket(packet.Packet) error
	WriteCommandPacket(packet.Packet) error

	WriteEmptyOK() error
	WriteError(error) error
	WriteMySQLError(error) error

	Close() error
	Closed() bool
}

type mysqlConn struct {
	conn    net.Conn
	tlsConn net.Conn
	useTLS  bool

	sequence int
	closed   bool

	connId       uint32 // only for server
	capabilities core.CapabilityFlag
}

func NewClientConnection(conn net.Conn, capabilities core.CapabilityFlag) Conn {
	return &mysqlConn{
		conn:         conn,
		sequence:     -1,
		capabilities: capabilities,
	}
}

func NewServerConnection(conn net.Conn, connId uint32, capabilities core.CapabilityFlag) Conn {
	return &mysqlConn{
		conn:         conn,
		sequence:     -1,
		connId:       connId,
		capabilities: capabilities,
	}
}

//func (c *mysqlConn) WithCapabilities(capabilities core.CapabilityFlag) Conn {
//	return &mysqlConn{
//		conn:         c.conn,
//		tlsConn:      c.tlsConn,
//		useTLS:       c.useTLS,
//		sequence:     c.sequence,
//		closed:       c.closed,
//		connId:       c.connId,
//		capabilities: capabilities,
//	}
//}

func (c *mysqlConn) SetCapabilities(capabilities core.CapabilityFlag) {
	c.capabilities = capabilities
}

func (c *mysqlConn) ClientTLS(config *tls.Config) {
	c.tlsConn = tls.Client(c.conn, config)
	c.useTLS = true
}

func (c *mysqlConn) ServerTLS(config *tls.Config) {
	c.tlsConn = tls.Server(c.conn, config)
	c.useTLS = true
}

func (c *mysqlConn) TLS() bool {
	return c.useTLS
}

func (c *mysqlConn) Capabilities() core.CapabilityFlag {
	return c.capabilities
}

func (c *mysqlConn) ConnectionId() uint32 {
	return c.connId
}

func (c *mysqlConn) RemoteAddr() net.Addr {
	return c.getConnection().RemoteAddr()
}

func (c *mysqlConn) Host() {
	c.getConnection().LocalAddr()
}

func (c *mysqlConn) ReadPacket() ([]byte, error) {
	// payload length
	lenData, err := c.next(3)
	if err != nil {
		return nil, err
	}
	length := packet.FixedLengthInteger.Get(lenData)

	// sequence
	seqData, err := c.next(1)
	if err != nil {
		return nil, err
	}
	if len(seqData) == 0 {
		return nil, packet.ErrPacketData
	}
	c.sequence = int(seqData[0])

	// payload
	payloadData, err := c.next(int(length))
	if err != nil {
		return nil, err
	}

	// packet bytes
	pktData := append(lenData, seqData...)
	pktData = append(pktData, payloadData...)

	// TODO
	fmt.Println(hex.Dump(pktData))

	return pktData, nil
}

func (c *mysqlConn) WritePacket(packet packet.Packet) error {
	c.sequence++
	packet.SetSequence(c.sequence)
	dump, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	// TODO
	fmt.Println(hex.Dump(dump))
	_, err = c.getConnection().Write(dump)
	return err
}

func (c *mysqlConn) WriteCommandPacket(packet packet.Packet) error {
	c.sequence = 0
	packet.SetSequence(c.sequence)
	dump, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	// TODO
	fmt.Println(hex.Dump(dump))
	_, err = c.getConnection().Write(dump)
	return err
}

func (c *mysqlConn) WriteEmptyOK() error {
	return c.WritePacket(&packet.OK{
		OKHeader:            0x00,
		AffectedRows:        0,
		LastInsertId:        0,
		StatusFlags:         0,
		WarningCount:        0,
		Info:                nil,
		SessionStateChanges: nil,
	})
}

func (c *mysqlConn) WriteError(err error) error {
	if err == nil {
		return fmt.Errorf("error required not nil")
	}

	if mysqlErr, ok := err.(errors.Error); ok {
		return c.WritePacket(mysqlErr.Packet())
	}

	mysqlErr := errors.NewWithoutSQLState(code.Err, err.Error())
	return c.WritePacket(mysqlErr.Packet())
}

func (c *mysqlConn) WriteMySQLError(err error) error {
	if err == nil {
		return fmt.Errorf("error required not nil")
	}

	if mysqlErr, ok := err.(errors.Error); ok {
		return c.WritePacket(mysqlErr.Packet())
	}
	return nil
}

func (c *mysqlConn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	if c.useTLS {
		return c.tlsConn.Close()
	}

	return c.conn.Close()
}

func (c *mysqlConn) Closed() bool {
	return c.closed
}

func (c *mysqlConn) getConnection() net.Conn {
	if c.useTLS {
		return c.tlsConn
	}
	return c.conn
}

func (c *mysqlConn) next(n int) ([]byte, error) {
	bs := make([]byte, n)
	_, err := c.getConnection().Read(bs)
	if err != nil {
		return nil, err
	}
	return bs, nil
}
