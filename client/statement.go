package client

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
)

func (c *conn) Exec(query string) (*mysql.Result, error) {
	pkt := packet.New(core.ComQuery, []byte(query))
	if err := c.WriteCommandPacket(pkt); err != nil {
		return nil, err
	}

	columnCount, err := c.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	if columnCount > 0 {
		// columnCount * ColumnDefinition packet
		if err := c.readUntilEOFPacket(); err != nil {
			return nil, err
		}

		// columnCount * ResultSetRow packet
		if err := c.readUntilEOFPacket(); err != nil {
			return nil, err
		}
	}

	if err := c.getResult(); err != nil {
		return nil, err
	}

	return mysql.NewResult(c.affectedRows, c.lastInsertId), nil
}

func (c *conn) Query(query string) (Rows, error) {
	pkt := packet.New(core.ComQuery, []byte(query))
	if err := c.WriteCommandPacket(pkt); err != nil {
		return nil, err
	}

	columnCount, err := c.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	rows := new(rows)
	rows.conn = c

	if columnCount > 0 {
		rows.columns, err = c.readColumns(columnCount)
	} else {
		rows.done = true
	}

	return rows, err
}

func (c *conn) readExecuteResponseFirstPacket() (int, error) {
	data, err := c.mysqlConn.ReadPacket()
	if err != nil {
		return 0, err
	}

	switch {
	case packet.IsOK(data) || packet.IsErr(data):
		return 0, c.handleOKERRPacket(data)

	case packet.IsLocalInfileRequest(data):
		// TODO
		return 0, fmt.Errorf("unsupported LOCAL INFILE Request")

	default:
		columnCount, err := packet.ParseColumnCount(data)
		if err != nil {
			return 0, err
		}
		return int(columnCount), nil
	}
}

func (c *conn) getResult() error {
	for c.status&core.ServerMoreResultsExists != 0 {
		columnCount, err := c.readExecuteResponseFirstPacket()
		if err != nil {
			return nil
		}

		if columnCount > 0 {
			if err := c.readUntilEOFPacket(); err != nil {
				return err
			}

			if err := c.readUntilEOFPacket(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *conn) readColumns(count int) ([]packet.Column, error) {
	var columns []packet.Column
	for i := 0; i < count; i++ {
		data, err := c.ReadPacket()
		if err != nil {
			return nil, err
		}

		columnDefPkt, err := packet.ParseColumnDefinition(data)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnDefPkt)
	}

	// TODO EOF deprecated
	if _, err := c.mysqlConn.ReadPacket(); err != nil {
		return nil, err
	}

	return columns, nil
}
