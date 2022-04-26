package mysql

import (
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/myerrors"
	"github.com/vczyh/mysql-protocol/packet"
	"net"
)

type Conn interface {
	SetCapabilities(capabilities flag.Capability)

	ClientTLS(config *tls.Config)
	ServerTLS(config *tls.Config)
	TLSed() bool

	ConnectionId() uint32
	Capabilities() flag.Capability

	RemoteAddr() net.Addr

	ReadPacket() ([]byte, error)

	WritePacket(packet.Packet) error
	WriteCommandPacket(packet.Packet) error

	WriteEmptyOK() error
	WriteError(error) error

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
	capabilities flag.Capability
}

func NewClientConnection(conn net.Conn, capabilities flag.Capability) Conn {
	return &mysqlConn{
		conn:         conn,
		sequence:     -1,
		capabilities: capabilities,
	}
}

func NewServerConnection(conn net.Conn, connId uint32, capabilities flag.Capability) Conn {
	return &mysqlConn{
		conn:         conn,
		sequence:     -1,
		connId:       connId,
		capabilities: capabilities,
	}
}

func (c *mysqlConn) SetCapabilities(capabilities flag.Capability) {
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

func (c *mysqlConn) TLSed() bool {
	return c.useTLS
}

func (c *mysqlConn) Capabilities() flag.Capability {
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
	//fmt.Println(hex.Dump(pktData))

	return payloadData, nil
}

func (c *mysqlConn) WritePacket(packet packet.Packet) error {
	c.sequence++

	data, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	headerData := c.buildPacketHeader(len(data))
	pktData := append(headerData, data...)

	// TODO
	fmt.Println(hex.Dump(pktData))
	return c.write(pktData)
}

func (c *mysqlConn) WriteCommandPacket(packet packet.Packet) error {
	c.sequence = 0
	data, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	headerData := c.buildPacketHeader(len(data))
	pktData := append(headerData, data...)

	// TODO
	//fmt.Println(hex.Dump(pktData))
	return c.write(pktData)
}

func (c *mysqlConn) write(pktData []byte) error {
	_, err := c.getConnection().Write(pktData)
	return err
}

func (c *mysqlConn) buildPacketHeader(len int) []byte {
	payloadLen := packet.FixedLengthInteger.Dump(uint64(len), 3)
	pktSeq := packet.FixedLengthInteger.Dump(uint64(c.sequence), 1)

	b := make([]byte, 4)
	copy(b, payloadLen)
	copy(b[3:], pktSeq)
	return b
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
	if !myerrors.Is(err) {
		return nil
	}

	if myerrors.CanSendToClient(err) {
		return c.WritePacket(packet.NewERR(err))
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
