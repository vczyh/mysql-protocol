package command

import "testing"

func TestTableColumnType(t *testing.T) {
	t.Logf("%x", MYSQL_TYPE_DECIMAL)
	t.Logf("%x", MYSQL_TYPE_NEWDECIMAL)
	t.Logf("%x", MYSQL_TYPE_ENUM)
	t.Logf("%x", MYSQL_TYPE_GEOMETRY)
}
