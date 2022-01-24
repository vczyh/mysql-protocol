package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"mysql-protocol/client"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/generic"
)

type conn struct {
	config    *config
	mysqlConn *client.Conn
}

type config struct {
	host     string
	port     int
	user     string
	password string
}

func createConnection(config *config) (driver.Conn, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	conn := &conn{config: config}
	var err error
	conn.mysqlConn, err = client.CreateConnection(
		client.WithHost(conn.config.host),
		client.WithPort(conn.config.port),
		client.WithUser(conn.config.user),
		client.WithPassword(conn.config.password))

	return conn, err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	pkt := command.NewStmtPrepare(query)
	if err := c.mysqlConn.WriteCommandPacket(pkt); err != nil {
		return nil, err
	}

	data, err := c.mysqlConn.ReadPacket()
	if err != nil {
		return nil, err
	}

	switch {
	case generic.IsErr(data):
		return nil, c.mysqlConn.HandleOKErrPacket(data)
	default:
		pkt, err := command.ParseStmtPrepareOKFirst(data)
		if err != nil {
			return nil, err
		}

		if pkt.ParamCount > 0 {
			if err := c.mysqlConn.ReadUntilEOFPacket(); err != nil {
				return nil, err
			}
		}

		if pkt.ColumnCount > 0 {
			if err := c.mysqlConn.ReadUntilEOFPacket(); err != nil {
				return nil, err
			}
		}

		stmt := &Stmt{
			conn:       c,
			id:         pkt.StmtId,
			paramCount: int(pkt.ParamCount),
		}
		return stmt, nil
	}
}

func (c *conn) Close() error {
	return c.mysqlConn.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	// TODO
	panic("implement me")
}

func (c *conn) Ping(ctx context.Context) error {
	// TODO context
	if err := c.mysqlConn.Ping(); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) getResult() error {
	for c.mysqlConn.Status&generic.SERVER_MORE_RESULTS_EXISTS != 0 {
		columnCount, err := c.readExecuteResponseFirstPacket()
		if err != nil {
			return nil
		}

		if columnCount > 0 {
			if err := c.mysqlConn.ReadUntilEOFPacket(); err != nil {
				return err
			}

			if err := c.mysqlConn.ReadUntilEOFPacket(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *conn) readExecuteResponseFirstPacket() (int, error) {
	data, err := c.mysqlConn.ReadPacket()
	if err != nil {
		return 0, err
	}

	switch {
	case generic.IsOK(data) || generic.IsErr(data):
		return 0, c.mysqlConn.HandleOKErrPacket(data)

	case generic.IsLocalInfileRequest(data):
		// TODO
		return 0, fmt.Errorf("unsupported LOCAL INFILE Request")

	default:
		columnCount, err := command.ParseQueryResponse(data)
		if err != nil {
			return 0, err
		}
		return int(columnCount), nil
	}
}

func (c *conn) readColumns(count int) ([]*command.ColumnDefinition, error) {
	columns := make([]*command.ColumnDefinition, count)

	for i := 0; i < count; i++ {
		data, err := c.mysqlConn.ReadPacket()
		if err != nil {
			return nil, err
		}

		columnDefPkt, err := command.ParseColumnDefinition(data)
		if err != nil {
			return nil, err
		}

		//columns[i].name = string(columnDefPkt.Name)
		//columns[i].length = columnDefPkt.ColumnLength
		//columns[i].columnType = command.TableColumnType(columnDefPkt.ColumnType)
		//columns[i].flags = generic.ColumnDefinitionFlag(columnDefPkt.Flags)
		columns[i] = columnDefPkt
		// TODO
	}

	// EOF TODO deprecated
	if _, err := c.mysqlConn.ReadPacket(); err != nil {
		return nil, err
	}

	return columns, nil
}
