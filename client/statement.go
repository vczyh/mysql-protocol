package client

import (
	"database/sql/driver"
	"mysql-protocol/packet/command"
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

}

func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	panic("implement me")
}
