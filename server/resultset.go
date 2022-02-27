package server

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"time"
)

type ResultSet interface {
	Columns() []packet.Column
	Rows() []packet.Row
	WriteText(mysql.Conn) error
}

type resultSet struct {
	columns []packet.Column
	rows    []packet.Row
}

func NewSimpleResultSet(columnNames []string, rowValues [][]interface{}) (ResultSet, error) {
	rs := new(resultSet)

	// column definition
	columns := make([]*packet.ColumnDefinition, len(columnNames))
	for i, name := range columnNames {
		columns[i] = &packet.ColumnDefinition{Name: name}
	}
	for _, rowValue := range rowValues {
		if len(columnNames) != len(rowValue) {
			return nil, fmt.Errorf("column num and row value num do not match")
		}

		for i, val := range rowValue {
			column := columns[i]
			bef := column.ColumnType
			if err := fillColumnDefinition(val, column); err != nil {
				return nil, err
			}
			now := column.ColumnType

			if i > 0 && now != bef {
				if now == packet.MySQLTypeNull {
					columns[i].ColumnType = bef
				} else if bef != packet.MySQLTypeNull && now != packet.MySQLTypeNull {
					return nil, fmt.Errorf("row value for same column type differ")
				}
			}
		}
	}
	rs.columns = make([]packet.Column, len(columns))
	for i, column := range columns {
		rs.columns[i] = column
	}

	// row value
	for _, rowValue := range rowValues {
		row := make(packet.Row, len(columnNames))
		for i, val := range rowValue {
			row[i] = packet.NewColumnValue(val == nil, val, columns[i].ColumnType)
		}
		rs.rows = append(rs.rows, row)
	}

	return rs, nil
}

func (rs *resultSet) Columns() []packet.Column {
	return rs.columns
}

func (rs *resultSet) Rows() []packet.Row {
	return rs.rows
}

func (rs *resultSet) WriteText(conn mysql.Conn) error {
	// column count packet
	columnCountPkt, err := packet.NewColumnCount(len(rs.columns))
	if err != nil {
		return err
	}
	if err := conn.WritePacket(columnCountPkt); err != nil {
		return err
	}

	// columnCount * ColumnDefinition packet
	for _, column := range rs.columns {
		if err := conn.WritePacket(column); err != nil {
			return err
		}
	}

	// EOF
	// TODO  CLIENT_DEPRECATE_EOF
	if err := conn.WritePacket(packet.NewEOF(0, 0)); err != nil {
		return err
	}

	// columnCount * ResultSetRow packet
	for _, row := range rs.rows {
		if err := conn.WritePacket(packet.NewTextResultSetRow(row)); err != nil {
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

func fillColumnDefinition(val interface{}, cd *packet.ColumnDefinition) error {
	cd.CharacterSet = charset.Utf8mb40900AiCi

	switch val.(type) {
	case int, int8, int16, int32, int64:
		cd.ColumnType = packet.MySQLTypeLongLong
		cd.Flags |= flag.BinaryFlag
	case uint, uint8, uint16, uint32, uint64:
		cd.ColumnType = packet.MySQLTypeLongLong
		cd.Flags |= flag.UnsignedFlag | flag.BinaryFlag
	case float32, float64:
		cd.ColumnType = packet.MySQLTypeDouble
		cd.Flags |= flag.BinaryFlag
	case string, []byte:
		cd.ColumnType = packet.MySQLTypeVarString
	case nil:
		cd.ColumnType = packet.MySQLTypeNull
	case time.Time:
		cd.ColumnType = packet.MySQLTypeDatetime
	default:
		return fmt.Errorf("unsupported column value type %T", val)
	}

	return nil
}
