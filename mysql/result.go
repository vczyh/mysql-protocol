package mysql

import (
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/packet"
)

type Result struct {
	AffectedRows uint64
	LastInsertId uint64
	Status       flag.Status
	WarningCount int
}

func (r *Result) Write(conn Conn) error {
	return conn.WritePacket(&packet.OK{
		OKHeader:     0x00,
		AffectedRows: r.AffectedRows,
		LastInsertId: r.LastInsertId,
		StatusFlags:  0,
		WarningCount: 0,
	})
}
