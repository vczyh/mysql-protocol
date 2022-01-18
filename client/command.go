package client

import (
	"database/sql/driver"
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
	Columns []*command.ColumnDefinition
	Rows    [][]*command.Value
}

func (rs *ResultSet) ColumnNames() (columns []string) {
	for _, column := range rs.Columns {
		columns = append(columns, string(column.Name))
	}
	return columns
}

// Query is implement of the COM_QUERY
func (c *Conn) Query(query string) (*ResultSet, error) {
	pkt := command.NewQuery(query)
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
	case generic.IsLocalInfileRequest(data)z:
		// TODO implement
		return nil, fmt.Errorf("unsupported LOCAL INFILE Request")
	default:
		return c.handleResultSet(data)
	}
}

// TODO MySQL 8.0.27 not work
// CreateDB is implement of the COM_CREATE_DB
func (c *Conn) CreateDB(db string) error {
	pkt := command.NewCreateDB(db)
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

// TODO MySQL 8.0.27 not work
// DropDB is implement of the COM_DROP_DB
func (c *Conn) DropDB(db string) error {
	pkt := command.NewDropDB(db)
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

// TODO MySQL 8.0.27 not work
func (c *Conn) Shutdown() error {
	pkt := command.NewShutdown()
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}

	data, err := c.readPacket()
	if err != nil {
		return err
	}
	switch {
	case generic.IsErr(data):
		return c.handleOKErrPacket(data)
	case generic.IsEOF(data):
		return nil
	default:
		return generic.ErrPacketData
	}
}

// Statistics is implement of the COM_CREATE_DB
func (c *Conn) Statistics() (string, error) {
	pkt := command.NewStatistics()
	if err := c.writeCommandPacket(pkt); err != nil {
		return "", err
	}

	data, err := c.readPacket()
	if err != nil {
		return "", err
	}
	switch {
	case generic.IsErr(data):
		return "", c.handleOKErrPacket(data)
	default:
		return string(data[4:]), nil
	}
}

// TODO MySQL 8.0.27 not work
func (c *Conn) ProcessInfo() (*ResultSet, error) {
	pkt := command.NewProcessInfo()
	if err := c.writeCommandPacket(pkt); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	switch {
	case generic.IsErr(data):
		return nil, c.handleOKErrPacket(data)
	default:
		return c.handleResultSet(data)
	}
}

// ProcessKill is implement of the COM_PROCESS_KILL
func (c *Conn) ProcessKill(connectionId int) error {
	pkt := command.NewProcessKill(connectionId)
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

// TODO 没报错没效果
// General log
// 2022-01-17T10:06:33.163277Z	   22 Debug
func (c *Conn) Debug() error {
	pkt := command.NewDebug()
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}

	data, err := c.readPacket()
	if err != nil {
		return err
	}
	switch {
	case generic.IsErr(data):
		return c.handleOKErrPacket(data)
	case generic.IsEOF(data):
		return nil
	default:
		return generic.ErrPacketData
	}
}

// Ping is implement of the COM_PING
func (c *Conn) Ping() error {
	pkt := command.NewPing()
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

// ResetConnection is implement of the COM_RESET_CONNECTION
func (c *Conn) ResetConnection() error {
	pkt := command.NewResetConnection()
	if err := c.writeCommandPacket(pkt); err != nil {
		return err
	}
	return c.readOKErrPacket()
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	pkt := command.NewStmtPrepare(query)
	if err := c.writeCommandPacket(pkt); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	switch {
	case generic.IsErr(data):
		return nil, c.handleOKErrPacket(data)
	default:
		pkt, err := command.ParseStmtPrepareOKFirst(data)
		if err != nil {
			return nil, err
		}

		if pkt.ParamCount > 0 {
			if err := c.readUntilEOFPacket(); err != nil {
				return nil, err
			}
		}

		if pkt.ColumnCount > 0 {
			if err := c.readUntilEOFPacket(); err != nil {
				return nil, err
			}
		}

		stmt := &Stmt{
			conn: c,
		}
		return stmt, nil
	}
}

func (c *Conn) handleResultSet(data []byte) (*ResultSet, error) {
	columnCount, err := command.ParseQueryResponse(data)
	if err != nil {
		return nil, err
	}

	rs := new(ResultSet)
	for i := 0; i < int(columnCount); i++ {
		if data, err = c.readPacket(); err != nil {
			return nil, err
		}
		columnDefPkt, err := command.ParseColumnDefinition(data)
		if err != nil {
			return nil, err
		}
		rs.Columns = append(rs.Columns, columnDefPkt)
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
