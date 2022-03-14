package client

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
)

func (c *conn) Exec(query string) (rs mysql.Result, err error) {
	if err := c.WriteCommandPacket(packet.NewCmd(packet.ComQuery, []byte(query))); err != nil {
		return rs, err
	}

	columnCount, err := c.readExecuteResponseFirstPacket()
	if err != nil {
		return rs, err
	}

	if columnCount > 0 {
		// columnCount * ColumnDefinition packet
		if err := c.readUntilEOFPacket(); err != nil {
			return rs, err
		}
		// n * ResultSetRow packet
		if err := c.readUntilEOFPacket(); err != nil {
			return rs, err
		}
	}

	if err := c.getResult(); err != nil {
		return rs, err
	}

	rs.AffectedRows = c.affectedRows
	rs.LastInsertId = c.lastInsertId
	return rs, nil
}

func (c *conn) Query(query string) (*Rows, error) {
	if err := c.WriteCommandPacket(packet.NewCmd(packet.ComQuery, []byte(query))); err != nil {
		return nil, err
	}

	columnCount, err := c.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	rows := new(Rows)
	rows.conn = c

	if columnCount > 0 {
		rows.columnDefs, rows.columns, err = c.readColumns(columnCount)
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
	for c.status&flag.ServerMoreResultsExists != 0 {
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

func (c *conn) readColumns(count int) ([]*packet.ColumnDefinition, []mysql.Column, error) {
	columnDefs := make([]*packet.ColumnDefinition, count)
	columns := make([]mysql.Column, count)

	for i := 0; i < count; i++ {
		data, err := c.ReadPacket()
		if err != nil {
			return nil, nil, err
		}
		columnDefPkt, err := packet.ParseColumnDefinition(data)
		if err != nil {
			return nil, nil, err
		}

		columnDefs[i] = columnDefPkt
		columns[i] = mysql.Column{
			Database: columnDefPkt.Schema,
			Table:    columnDefPkt.Table,
			Name:     columnDefPkt.Name,
			CharSet:  columnDefPkt.CharacterSet,
			Length:   columnDefPkt.ColumnLength,
			Type:     columnDefPkt.ColumnType,
			Flags:    columnDefPkt.Flags,
			Decimals: columnDefPkt.Decimals,
		}
	}

	// TODO EOF deprecated
	if _, err := c.mysqlConn.ReadPacket(); err != nil {
		return nil, nil, err
	}
	return columnDefs, columns, nil
}
