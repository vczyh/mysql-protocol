package client

import (
	"database/sql/driver"
	"fmt"
	"math"
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/types"
	"time"
)

type Stmt struct {
	conn       *Conn
	id         uint32
	paramCount int
}

// TODO idempotent
func (stmt *Stmt) Close() error {
	if stmt.conn == nil {
		return nil
	}

	pkt := command.NewStmtCLost(stmt.id)
	if err := stmt.conn.writeCommandPacket(pkt); err != nil {
		return err
	}

	stmt.conn = nil
	return nil
}

func (stmt *Stmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	// TODO handle case: stmt has been closed

	if err := stmt.writeExecutePacket(args); err != nil {
		return nil, err
	}

	stmt.conn.affectedRows = 0
	stmt.conn.lastInsertId = 0

	columnCount, err := stmt.conn.readExecuteResponseFirstPacket()
	if err != nil {
		return nil, err
	}

	if columnCount > 0 {
		if err := stmt.conn.readUntilEOFPacket(); err != nil {
			return nil, err
		}
		if err := stmt.conn.readUntilEOFPacket(); err != nil {
			return nil, err
		}
	}

	if err := stmt.conn.getResult(); err != nil {
		return nil, err
	}

	return &result{
		affectedRows: int64(stmt.conn.affectedRows),
		lastInsertId: int64(stmt.conn.lastInsertId),
	}, nil
}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO handle case: stmt has been closed

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
		// TODO done variable
		// TODO 没有column 可能是update语句等
	}
	return rows, nil
}

func (stmt *Stmt) writeExecutePacket(args []driver.Value) (err error) {
	pkt := command.NewStmtExecute()

	pkt.StmtId = stmt.id
	pkt.Flags = 0x00 // CURSOR_TYPE_NO_CURSOR
	pkt.IterationCount = 1

	fmt.Println("stmtId:", pkt.StmtId) // TODO

	if n := len(args); n > 0 {
		offset := 0
		pkt.NewParamsBoundFlag = 0x01
		pkt.CreateNullBitMap(n, offset)

		for i, arg := range args {
			switch v := arg.(type) {
			case nil:
				pkt.NullBitMapSet(n, i, offset)
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_NULL), 0x00)
				continue

			case uint64:
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_LONGLONG), 0x80)
				pkt.ParamValue = append(pkt.ParamValue, types.FixedLengthInteger.Dump(v, 8)...)

			case float64:
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_DOUBLE), 0x00)
				floatData := math.Float64bits(v)
				pkt.ParamValue = append(pkt.ParamValue, types.FixedLengthInteger.Dump(floatData, 8)...)

			case bool:
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_TINY), 0x00)
				if v {
					pkt.ParamValue = append(pkt.ParamValue, 0x01)
				} else {
					pkt.ParamValue = append(pkt.ParamValue, 0x00)
				}

			case []byte:
				if v == nil {
					pkt.NullBitMapSet(n, i, offset)
					pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_NULL), 0x00)
					continue
				}
				// TODO long data
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_STRING), 0x00)
				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedInteger.Dump(uint64(len(v)))...)
				pkt.ParamValue = append(pkt.ParamValue, v...)

			case string:
				// TODO long data
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_STRING), 0x00)
				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedString.Dump([]byte(v))...)
				//pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedInteger.Dump(uint64(len(v)))...)
				//pkt.ParamValue = append(pkt.ParamValue, v...)

			case time.Time:
				pkt.ParamType = append(pkt.ParamType, byte(command.MYSQL_TYPE_STRING), 0x00)

				var a [64]byte
				var b = a[:0]

				if v.IsZero() {
					b = append(b, "0000-00-00"...)
				} else {
					b, err = appendDateTime(b, v.In(time.Local)) // TODO time location
					if err != nil {
						return err
					}
				}

				pkt.ParamValue = append(pkt.ParamValue, types.LengthEncodedInteger.Dump(uint64(len(b)))...)
				pkt.ParamValue = append(pkt.ParamValue, b...)

			default:
				return fmt.Errorf("cannot convert type: %T", arg)
			}
		}
	}

	return stmt.conn.writeCommandPacket(pkt)
}
