package client

import (
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/packet"
	"io"
)

type Rows interface {
	Columns() []packet.Column

	Next() (packet.Row, error)

	// HasNextResultSet is called at the end of the current result set and
	// reports whether there is another result set after the current one.
	HasNextResultSet() bool

	// NextResultSet advances the driver to the next result set even
	// if there are remaining rows in the current result set.
	//
	// NextResultSet should return io.EOF when there are no more result sets.
	NextResultSet() error
}

type rows struct {
	conn    *conn
	columns []packet.Column

	// current result set packet is read off or not
	done bool
}

func (r *rows) Columns() []packet.Column {
	return r.columns
}

func (r *rows) Next() (packet.Row, error) {
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
		return packet.ParseTextResultSetRow(data, r.columns, r.conn.loc)
	}
}

func (r *rows) HasNextResultSet() bool {
	return r.conn.status&flag.ServerMoreResultsExists != 0
}

func (r *rows) NextResultSet() error {
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
			r.columns, err = r.conn.readColumns(columnCount)
			return err
		}
	}
}
