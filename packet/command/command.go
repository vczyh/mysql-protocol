package command

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
)

func NewQuit() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComQuit})
}

func NewInitDB(db string) *generic.Simple {
	data := append([]byte{generic.ComInitDB}, db...)
	return generic.NewSimple(data)
}

func NewQuery(query string) *generic.Simple {
	data := append([]byte{generic.ComQuery}, query...)
	return generic.NewSimple(data)
}

func ParseQueryResponse(data []byte) (uint64, error) {
	if len(data) < 5 {
		return 0, generic.ErrPacketData
	}
	buf := bytes.NewBuffer(data[4:])
	columnCount, err := types.LengthEncodedInteger.Get(buf)
	return columnCount, err
}

func NewCreateDB(db string) *generic.Simple {
	data := append([]byte{generic.ComCreateDB}, db...)
	return generic.NewSimple(data)
}

func NewDropDB(db string) *generic.Simple {
	data := append([]byte{generic.ComDropDB}, db...)
	return generic.NewSimple(data)
}

func NewShutdown() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComShutdown, 0x10})
}

func NewStatistics() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComStatistics})
}

func NewProcessInfo() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComProcessInfo})
}

func NewProcessKill(connectionId int) *generic.Simple {
	connectionIdData := types.FixedLengthInteger.Dump(uint64(connectionId), 4)
	data := append([]byte{generic.ComProcessKill}, connectionIdData...)
	return generic.NewSimple(data)
}

func NewDebug() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComDebug})
}

func NewPing() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComPing})
}

func NewResetConnection() *generic.Simple {
	return generic.NewSimple([]byte{generic.ComResetConnection})
}

func NewStmtPrepare(query string) *generic.Simple {
	data := append([]byte{generic.ComStmtPrepare}, query...)
	return generic.NewSimple(data)
}

func NewStmtClose(stmtId uint32) *generic.Simple {
	stmtIdData := types.FixedLengthInteger.Dump(uint64(stmtId), 4)
	data := append([]byte{generic.ComStmtClose}, stmtIdData...)
	return generic.NewSimple(data)
}
