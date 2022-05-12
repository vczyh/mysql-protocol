package packet

import (
	"github.com/vczyh/mysql-protocol/flag"
	"testing"
)

func TestTableColumnType(t *testing.T) {
	t.Log(uint8(flag.MySQLTypeInvalid))
	t.Log(uint8(flag.MySQLTypeJson))
	t.Log(uint8(flag.MySQLTypeGeometry))
	t.Log(uint8(flag.MySQLTypeString))
}
