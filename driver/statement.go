package driver

import (
	"database/sql/driver"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/command"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
	"math"
	"reflect"
	"time"
)

type Stmt struct {
	conn       *conn
	id         uint32
	paramCount int
}

func (stmt *Stmt) Close() error {
	if stmt.conn == nil {
		return nil
	}

	pkt := command.NewStmtClose(stmt.id)
	if err := stmt.conn.mysqlConn.WriteCommandPacket(pkt); err != nil {
		return err
	}

	stmt.conn = nil
	return nil
}

func (stmt *Stmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := stmt.writeExecutePacket(args); err != nil {
		return nil, err
	}

	// TODO lastInsertId affectedRows reseted?
	columnCount, err := stmt.conn.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	if columnCount > 0 {
		// columnCount * ColumnDefinition packet
		if err := stmt.conn.mysqlConn.ReadUntilEOFPacket(); err != nil {
			return nil, err
		}

		// columnCount * BinaryResultSetRow packet
		if err := stmt.conn.mysqlConn.ReadUntilEOFPacket(); err != nil {
			return nil, err
		}
	}

	if err := stmt.conn.getResult(); err != nil {
		return nil, err
	}

	return &result{
		affectedRows: int64(stmt.conn.mysqlConn.AffectedRows()),
		lastInsertId: int64(stmt.conn.mysqlConn.LastInsertId()),
	}, nil
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := stmt.writeExecutePacket(args); err != nil {
		return nil, err
	}

	columnCount, err := stmt.conn.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	rows := new(binaryRows)
	rows.conn = stmt.conn

	if columnCount > 0 {
		rows.columns, err = stmt.conn.readColumns(columnCount)
	} else {
		rows.done = true
	}

	return rows, err
}

// CheckNamedValue refer to database/sql/driver/types.go:ConvertValue()
func (stmt *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if driver.IsValue(nv.Value) {
		return nil
	}

	switch vr := nv.Value.(type) {
	case driver.Valuer:
		sv, err := callValuerValue(vr)
		if err != nil {
			return err
		}
		if driver.IsValue(sv) {
			nv.Value = sv
			return nil
		}
		if u, ok := sv.(uint64); ok {
			nv.Value = u
			return nil
		}
		return fmt.Errorf("non-Value type %T returned from Value", sv)
	}

	rv := reflect.ValueOf(nv.Value)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			nv.Value = nil
		} else {
			nv.Value = rv.Elem().Interface()
			return stmt.CheckNamedValue(nv)
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		nv.Value = rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		nv.Value = rv.Uint()
	case reflect.Float32, reflect.Float64:
		nv.Value = rv.Float()
	case reflect.Bool:
		nv.Value = rv.Bool()

	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek == reflect.Uint8 {
			nv.Value = rv.Bytes()
		}
		return fmt.Errorf("unsupported type %T, a slice of %s", nv.Value, ek)

	case reflect.String:
		nv.Value = rv.String()
	default:
		return fmt.Errorf("unsupported type %T, a %s", nv.Value, rv.Kind())
	}

	return nil
}

func (stmt *Stmt) writeExecutePacket(args []driver.Value) (err error) {
	pkt := command.NewStmtExecute()

	pkt.StmtId = stmt.id
	pkt.Flags = 0x00 // CURSOR_TYPE_NO_CURSOR

	if n := len(args); n > 0 {
		pkt.NewParamsBoundFlag = 0x01
		pkt.CreateNullBitMap(n)

		for i, arg := range args {
			switch v := arg.(type) {
			case nil:
				pkt.NullBitMapSet(n, i)
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeNull), 0x00)

			case int64:
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeLongLong), 0x00)
				pkt.ParamValue = append(pkt.ParamValue, types.FixedLengthInteger.Dump(uint64(v), 8)...)

			case uint64:
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeLongLong), 0x80)
				pkt.ParamValue = append(pkt.ParamValue, types.FixedLengthInteger.Dump(v, 8)...)

			case float64:
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeDouble), 0x00)
				floatData := math.Float64bits(v)
				pkt.ParamValue = append(pkt.ParamValue, types.FixedLengthInteger.Dump(floatData, 8)...)

			case bool:
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeTiny), 0x00)
				if v {
					pkt.ParamValue = append(pkt.ParamValue, 0x01)
				} else {
					pkt.ParamValue = append(pkt.ParamValue, 0x00)
				}

			case []byte:
				if v == nil {
					pkt.NullBitMapSet(n, i)
					pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeNull), 0x00)
					continue
				}
				// TODO long data
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeString), 0x00)
				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedInteger.Dump(uint64(len(v)))...)
				pkt.ParamValue = append(pkt.ParamValue, v...)

			case string:
				// TODO long data
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeString), 0x00)
				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedString.Dump([]byte(v))...)

			case time.Time:
				pkt.ParamType = append(pkt.ParamType, byte(generic.MySQLTypeString), 0x00)

				var b []byte
				if v.IsZero() {
					b = append(b, "0000-00-00 00:00:00.000000"...)
				} else {
					b = append(b, v.Format("2006-01-02 15:04:05.000000")...)
				}

				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedInteger.Dump(uint64(len(b)))...)
				pkt.ParamValue = append(pkt.ParamValue, b...)

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}
	}

	return stmt.conn.mysqlConn.WriteCommandPacket(pkt)
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}
