package packet

import "testing"

func TestTableColumnType(t *testing.T) {
	t.Log(uint8(MySQLTypeInvalid))
	t.Log(uint8(MysSQLTypeJson))
	t.Log(uint8(MySQLTypeGeometry))
	t.Log(uint8(MySQLTypeString))
}