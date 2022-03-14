package client

import (
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"io"
)

type Rows struct {
	conn       *conn
	columnDefs []*packet.ColumnDefinition
	columns    []mysql.Column

	// current result set packet is read off or not
	done bool
}

func (r *Rows) Columns() []mysql.Column {
	return r.columns
}

func (r *Rows) Next() (mysql.Row, error) {
	if r.done {
		return nil, io.EOF
	}

	data, err := r.conn.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch {
	case packet.IsErr(data):
		return nil, r.conn.handleOKERRPacket(data)
	case packet.IsEOF(data):
		r.done = true
		return nil, io.EOF
	default:
		pktRow, err := packet.ParseTextResultSetRow(data, r.columnDefs, r.conn.loc)
		if err != nil {
			return nil, err
		}
		row := make(mysql.Row, len(pktRow))
		for i, pktColumnVal := range pktRow {
			row[i] = mysql.NewColumnValue(pktColumnVal.Value)
		}
		return row, nil
	}
}

func (r *Rows) HasNextResultSet() bool {
	return r.conn.status&flag.ServerMoreResultsExists != 0
}

func (r *Rows) NextResultSet() error {
	for {
		if !r.done {
			if err := r.conn.readUntilEOFPacket(); err != nil {
				return err
			}
			r.done = true
		}

		if !r.HasNextResultSet() {
			return io.EOF
		}

		columnCount, err := r.conn.readExecuteResponseFirstPacket()
		if err != nil {
			return err
		}
		if columnCount > 0 {
			r.done = false
			r.columnDefs, r.columns, err = r.conn.readColumns(columnCount)
			return err
		}
	}
}
