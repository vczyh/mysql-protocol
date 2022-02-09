package mysql

import (
	"encoding/hex"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
	"net"
)

type Conn interface {
	Capabilities() generic.CapabilityFlag

	ReadPacket() ([]byte, error)

	WritePacket(generic.Packet) error

	WriteCommandPacket(generic.Packet) error

	WriteEmptyOK() error

	Close() error
	Closed() bool
}

type mysqlConn struct {
	conn     net.Conn
	sequence int
	closed   bool

	capabilities generic.CapabilityFlag
}

func NewConnection(conn net.Conn, capabilities generic.CapabilityFlag) (*mysqlConn, error) {
	c := &mysqlConn{
		conn:         conn,
		sequence:     -1,
		capabilities: capabilities,
	}
	return c, nil
}

func (c *mysqlConn) Capabilities() generic.CapabilityFlag {
	return c.capabilities
}

func (c *mysqlConn) ReadPacket() ([]byte, error) {
	// payload length
	lenData, err := c.next(3)
	if err != nil {
		return nil, err
	}
	length := types.FixedLengthInteger.Get(lenData)

	// sequence
	seqData, err := c.next(1)
	if err != nil {
		return nil, err
	}
	if len(seqData) == 0 {
		return nil, generic.ErrPacketData
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

func (c *mysqlConn) WritePacket(packet generic.Packet) error {
	c.sequence++
	packet.SetSequence(c.sequence)
	dump, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	// TODO
	fmt.Println(hex.Dump(dump))
	_, err = c.conn.Write(dump)
	return err
}

func (c *mysqlConn) WriteCommandPacket(packet generic.Packet) error {
	c.sequence = 0
	packet.SetSequence(c.sequence)
	dump, err := packet.Dump(c.capabilities)
	if err != nil {
		return err
	}
	// TODO
	fmt.Println(hex.Dump(dump))
	_, err = c.conn.Write(dump)
	return err
}

func (c *mysqlConn) WriteEmptyOK() error {
	return c.WritePacket(&generic.OK{
		OKHeader:            0x00,
		AffectedRows:        0,
		LastInsertId:        0,
		StatusFlags:         0, // TODO
		WarningCount:        0,
		Info:                nil,
		SessionStateChanges: nil,
	})
}

func (c *mysqlConn) Close() error {
	c.closed = true
	return c.conn.Close()
}

func (c *mysqlConn) Closed() bool {
	return c.closed
}

func (c *mysqlConn) next(n int) ([]byte, error) {
	bs := make([]byte, n)
	_, err := c.conn.Read(bs)
	if err != nil {
		return nil, err
	}
	return bs, nil
}
