package server

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"time"
)

var (
	ErrColumnRowMismatch = errors.New("column num and row value num do not match")
)

//type ResultSet interface {
//	Columns() []packet.Column
//	Rows() []packet.Row
//	WriteText(mysql.Conn) error
//}

type ResultSet struct {
	columns []mysql.Column
	rows    []mysql.Row
}

func NewResultSet(columns []mysql.Column, rows []mysql.Row) (*ResultSet, error) {
	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, ErrColumnRowMismatch
		}
	}
	rs := new(ResultSet)
	rs.columns = columns
	rs.rows = rows
	return rs, nil
}

func NewSimpleResultSet(columnNames []string, rowValues [][]interface{}) (*ResultSet, error) {
	columns := make([]mysql.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = mysql.Column{Name: name}
	}

	for _, rowValue := range rowValues {
		if len(columnNames) != len(rowValue) {
			return nil, ErrColumnRowMismatch
		}
		for i, val := range rowValue {
			column := &columns[i]
			bef := column.Type
			if err := fillColumnDefinition(val, column); err != nil {
				return nil, err
			}

			now := column.Type
			if i > 0 && now != bef {
				if now == packet.MySQLTypeNull {
					column.Type = bef
				} else if bef != packet.MySQLTypeNull && now != packet.MySQLTypeNull {
					return nil, fmt.Errorf("row value for same column type differ")
				}
			}
		}
	}

	rows := make([]mysql.Row, len(rowValues))
	for i, rowValue := range rowValues {
		row := make(mysql.Row, len(columnNames))
		for j, val := range rowValue {
			row[j] = mysql.NewColumnValue(val)
		}
		rows[i] = row
	}

	return NewResultSet(columns, rows)
}

func (rs *ResultSet) Columns() []mysql.Column {
	return rs.columns
}

func (rs *ResultSet) Rows() []mysql.Row {
	return rs.rows
}

func (rs *ResultSet) WriteText(conn mysql.Conn) error {
	columnDefs := rs.columnDefinitionPackets()

	// column count packet
	columnCountPkt := packet.NewColumnCount(len(columnDefs))
	if err := conn.WritePacket(columnCountPkt); err != nil {
		return err
	}

	// columnCount * ColumnDefinition packet
	for _, column := range columnDefs {
		if err := conn.WritePacket(column); err != nil {
			return err
		}
	}

	// EOF
	// TODO  CLIENT_DEPRECATE_EOF
	if err := conn.WritePacket(packet.NewEOF(0, 0)); err != nil {
		return err
	}

	// columnCount * TextResultSetRow packet
	for _, row := range rs.textRowPackets() {
		if err := conn.WritePacket(row); err != nil {
			return err
		}
	}

	// EOF
	// TODO  CLIENT_DEPRECATE_EOF
	if err := conn.WritePacket(packet.NewEOF(0, 0)); err != nil {
		return err
	}

	return nil
}

func (rs *ResultSet) columnDefinitionPackets() []*packet.ColumnDefinition {
	columnDefs := make([]*packet.ColumnDefinition, len(rs.columns))
	for i, c := range rs.columns {
		columnDefs[i] = &packet.ColumnDefinition{
			Schema:       c.Database,
			Table:        c.Table,
			OrgTable:     c.OrgTable,
			Name:         c.Name,
			OrgName:      c.OrgName,
			CharacterSet: c.CharSet,
			ColumnLength: c.Length,
			ColumnType:   c.Type,
			Flags:        c.Flags,
			Decimals:     c.Decimals,
		}
	}
	return columnDefs
}

func (rs *ResultSet) textRowPackets() []*packet.TextResultSetRow {
	textRows := make([]*packet.TextResultSetRow, len(rs.rows))
	for i, row := range rs.rows {
		dRow := make([]packet.ColumnValue, len(row))
		for j, cv := range row {
			dRow[j] = packet.ColumnValue{Value: cv.Value()}
		}
		textRows[i] = &packet.TextResultSetRow{
			Row: dRow,
		}
	}
	return textRows
}

func fillColumnDefinition(val interface{}, column *mysql.Column) error {
	column.CharSet = charset.Utf8mb40900AiCi

	switch val.(type) {
	case int, int8, int16, int32, int64:
		column.Type = packet.MySQLTypeLongLong
		column.Flags |= flag.BinaryFlag
	case uint, uint8, uint16, uint32, uint64:
		column.Type = packet.MySQLTypeLongLong
		column.Flags |= flag.UnsignedFlag | flag.BinaryFlag
	case float32, float64:
		column.Type = packet.MySQLTypeDouble
		column.Flags |= flag.BinaryFlag
	case string, []byte:
		column.Type = packet.MySQLTypeVarString
	case nil:
		column.Type = packet.MySQLTypeNull
	case time.Time:
		column.Type = packet.MySQLTypeDatetime
	default:
		return fmt.Errorf("unsupported column value type %T", val)
	}

	return nil
}
