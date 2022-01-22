package client

import (
	"database/sql/driver"
	"io"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/generic"
	"time"
)

type resultSet struct {
	conn        *Conn
	columns     []*command.ColumnDefinition
	columnNames []string
}

func (rs *resultSet) Columns() []string {
	if rs.columnNames != nil {
		return rs.columnNames
	}

	names := make([]string, len(rs.columns))
	// TODO ColumnsWithAlias
	for i := range rs.columns {
		names[i] = string(rs.columns[i].Name)
	}

	rs.columnNames = names
	return rs.columnNames
}

func (rs *resultSet) Close() error {
	//TODO implement me
	panic("implement me")
}

type binaryRows struct {
	resultSet
}

//func (r *binaryRows) Columns() []string {
//	if r.columnNames != nil {
//		return r.columnNames
//	}
//
//	names := make([]string, len(r.columns))
//	// TODO ColumnsWithAlias
//	for i := range r.columns {
//		names[i] = string(r.columns[i].Name)
//	}
//
//	r.columnNames = names
//	return r.columnNames
//}

func (r *binaryRows) Next(dest []driver.Value) error {
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

type textRows struct {
	resultSet
}

func (r *textRows) Next(dest []driver.Value) error {
	data, err := r.conn.readPacket()
	if err != nil {
		return err
	}

	switch {
	case generic.IsErr(data):
		return r.conn.handleOKErrPacket(data)

	case generic.IsEOF(data):
		return io.EOF

	default:
		rowPkt, err := command.ParseTextResultSetRow(data)
		if err != nil {
			return err
		}

		for i := range dest {
			val := rowPkt.Values[i]
			if val.IsNull() {
				dest[i] = nil
			} else {
				dest[i] = val.String()
			}
		}
		return nil
	}
}
