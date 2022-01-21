package client

import (
	"database/sql/driver"
	"mysql-protocol/packet/command"
	"time"
)

type rows struct {
	conn *Conn
	//columns     []column
	columns     []*command.ColumnDefinition
	columnNames []string
}

func (r *rows) Columns() []string {
	if r.columnNames != nil {
		return r.columnNames
	}

	names := make([]string, len(r.columns))
	// TODO ColumnsWithAlias
	for i := range r.columns {
		names[i] = string(r.columns[i].Name)
	}

	r.columnNames = names
	return r.columnNames
}

func (r *rows) Close() error {
	panic("implement me")
}

func (r *rows) Next(dest []driver.Value) error {
	data, err := r.conn.readPacket()
	if err != nil {
		return err
	}

	rowPkt, err := command.ParseBinaryResultSetRow(data, len(dest))
	if err != nil {
		return err
	}
	// TODO location
	return rowPkt.Convert(dest, r.columns, time.Local)
}
