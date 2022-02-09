package server

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet/command"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"time"
)

type ResultSet struct {
	columns []*command.ColumnDefinition
	rows    [][]command.ColumnValue
}

func NewSimpleResultSet(columnNames []string, rowValues [][]interface{}) (*ResultSet, error) {
	rs := new(ResultSet)
	rs.columns = make([]*command.ColumnDefinition, len(columnNames))

	// column definition
	for i, name := range columnNames {
		rs.columns[i] = &command.ColumnDefinition{Name: name}
	}
	for _, rowValue := range rowValues {
		if len(columnNames) != len(rowValue) {
			return nil, fmt.Errorf("column num and row value num do not match")
		}

		for i, val := range rowValue {
			column := rs.columns[i]
			bef := column.ColumnType
			if err := fillColumnDefinition(val, column); err != nil {
				return nil, err
			}
			now := column.ColumnType

			if i > 0 && now != bef {
				if now == generic.MySQLTypeNull {
					rs.columns[i].ColumnType = bef
				} else if bef != generic.MySQLTypeNull && now != generic.MySQLTypeNull {
					return nil, fmt.Errorf("row value for same column type differ")
				}
			}
		}
	}

	// row value
	for _, rowValue := range rowValues {
		row := make([]command.ColumnValue, len(columnNames))
		for i, val := range rowValue {
			row[i] = command.NewColumnValue(val == nil, val, rs.columns[i].ColumnType)
		}
		rs.rows = append(rs.rows, row)
	}

	return rs, nil
}

func (rs *ResultSet) WriteText(conn mysql.Conn) error {
	// column count packet
	columnCountPkt, err := command.NewColumnCount(len(rs.columns))
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
	if err := conn.WritePacket(generic.NewEOF(0, 0)); err != nil {
		return err
	}

	// columnCount * ResultSetRow packet
	for _, row := range rs.rows {
		if err := conn.WritePacket(command.NewTextResultSetRow(row)); err != nil {
			return err
		}
	}

	// EOF
	// TODO  CLIENT_DEPRECATE_EOF
	if err := conn.WritePacket(generic.NewEOF(0, 0)); err != nil {
		return err
	}

	return nil
}

func fillColumnDefinition(val interface{}, cd *command.ColumnDefinition) error {
	cd.CharacterSet = generic.Utf8mb40900AiCi

	switch val.(type) {
	case int, int8, int16, int32, int64:
		cd.ColumnType = generic.MySQLTypeLongLong
		cd.Flags |= generic.BinaryFlag
	case uint, uint8, uint16, uint32, uint64:
		cd.ColumnType = generic.MySQLTypeLongLong
		cd.Flags |= generic.UnsignedFlag | generic.BinaryFlag
	case float32, float64:
		cd.ColumnType = generic.MySQLTypeDouble
		cd.Flags |= generic.BinaryFlag
	case string, []byte:
		cd.ColumnType = generic.MySQLTypeVarString
	case nil:
		cd.ColumnType = generic.MySQLTypeNull
	case time.Time: // TODO delete?
		cd.ColumnType = generic.MySQLTypeDatetime
	default:
		return fmt.Errorf("unsupported column value type %T", val)
	}

	return nil
}
