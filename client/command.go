package client

import (
	"encoding/hex"
	"fmt"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/generic"
)

// Quit is implement of the COM_QUIT
func (c *Conn) Quit() error {
	pkt := command.NewQuit()
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}

	data, err := c.readPacket()
	// response is either a connection close or a OK_Packet
	if err == nil && generic.IsOK(data) {
		_ = c.subConn.conn.Close()
		return nil
	}

	_ = c.subConn.conn.Close()
	return nil
}

// InitDB is implement of the COM_QUIT
func (c *Conn) InitDB(db string) error {
	pkt := command.NewInitDB(db)
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

type ResultSet struct {
	Columns []command.ColumnDefinition
	Rows    [][]string
}

// Execute is implement of the COM_QUERY
func (c *Conn) Execute(query string) (*ResultSet, error) {
	pkt := new(command.Query)
	pkt.SetQuery(query)
	if err := c.writeCommandPacket(pkt); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	switch {
	case generic.IsOK(data):
		return &ResultSet{}, nil
	case generic.IsErr(data):
		return nil, c.handleOKErrPacket(data)
	case generic.IsLocalInfileRequest(data):
		// TODO implement
		return nil, fmt.Errorf("unsupported LOCAL INFILE Request")
	default:
		return c.handleResultSet(data)
	}
}

func (c *Conn) handleResultSet(data []byte) (*ResultSet, error) {
	queryResPkt, err := command.ParseQueryResponse(data)
	if err != nil {
		return nil, err
	}

	rs := new(ResultSet)
	for i := 0; i < int(queryResPkt.ColumnCount); i++ {
		if data, err = c.readPacket(); err != nil {
			return nil, err
		}
		columnDefPkt, err := command.ParseColumnDefinition(data)
		if err != nil {
			return nil, err
		}
		rs.Columns = append(rs.Columns, *columnDefPkt)
	}

	// EOF TODO deprecated
	if data, err = c.readPacket(); err != nil {
		return nil, err
	}

	for {
		if data, err = c.readPacket(); err != nil {
			return nil, err
		}
		switch {
		case generic.IsErr(data):
			return nil, c.handleOKErrPacket(data)
		// TODO EOF deprecated
		case generic.IsEOF(data):
			return rs, nil
		default:
			resultSetRowPkt, err := command.ParseTextResultSetRow(data)
			if err != nil {
				return nil, err
			}
			rs.Rows = append(rs.Rows, resultSetRowPkt.GetValues())
		}
	}
}

func (c *Conn) writeCommandPacket(pkt generic.Packet) error {
	c.sequence = 0
	pkt.SetSequence(int(c.sequence))
	fmt.Println(hex.Dump(pkt.Dump())) // todo
	_, err := c.subConn.Write(pkt.Dump())
	return err
}
