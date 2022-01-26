package driver

import (
	"database/sql/driver"
	"io"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/generic"
)

type resultSet struct {
	conn        *conn
	columns     []*command.ColumnDefinition
	columnNames []string
	done        bool
}

func (rs *resultSet) Columns() []string {
	if rs.columnNames != nil {
		return rs.columnNames
	}

	names := make([]string, len(rs.columns))
	// TODO ColumnsWithAlias
	for i := range rs.columns {
		names[i] = rs.columns[i].Name
	}

	rs.columnNames = names
	return rs.columnNames
}

func (rs *resultSet) Close() error {
	if rs.done {
		return nil
	}

	err := rs.conn.mysqlConn.ReadUntilEOFPacket()
	if err != nil {
		return err
	}

	if err := rs.conn.getResult(); err != nil {
		return err
	}
	rs.done = true
	return nil
}

type binaryRows struct {
	resultSet
}

func (r *binaryRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}

	data, err := r.conn.mysqlConn.ReadPacket()
	if err != nil {
		return err
	}

	switch {
	case generic.IsErr(data):
		return r.conn.mysqlConn.HandleOKErrPacket(data)

	case generic.IsEOF(data):
		r.done = true
		return io.EOF

	default:
		rowPkt, err := command.ParseBinaryResultSetRow(data, r.columns, r.conn.config.loc)
		if err != nil {
			return err
		}
		for i := range dest {
			dest[i] = rowPkt.Values[i].Value()
		}
	}
	return nil
}

//type textRows struct {
//	resultSet
//}
//
//func (r *textRows) Next(dest []driver.Value) error {
//	if r.done {
//		return io.EOF
//	}
//
//	data, err := r.conn.mysqlConn.ReadPacket()
//	if err != nil {
//		return err
//	}
//
//	switch {
//	case generic.IsErr(data):
//		return r.conn.mysqlConn.HandleOKErrPacket(data)
//
//	case generic.IsEOF(data):
//		r.done = true
//		return io.EOF
//
//	default:
//		rowPkt, err := command.ParseTextResultSetRow(data, r.columns)
//		if err != nil {
//			return err
//		}
//
//		for i := range dest {
//			val := rowPkt.Values[i]
//			if val.IsNull() {
//				dest[i] = nil
//			} else {
//				dest[i] = val.Value()
//			}
//		}
//		return nil
//	}
//}
