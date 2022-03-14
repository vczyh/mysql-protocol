package mysql

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/packet"
	"strconv"
	"strings"
	"time"
)

type Column struct {
	Database string
	Table    string
	OrgTable string
	Name     string
	OrgName  string
	CharSet  *charset.Collation
	Length   uint32
	Type     packet.TableColumnType
	Flags    flag.ColumnDefinition
	Decimals byte
}

func (c *Column) String() string {
	var sb strings.Builder
	sb.WriteString("Database: " + c.Database)
	sb.WriteString(", ")

	sb.WriteString("Table: " + c.Table)
	sb.WriteString(", ")

	sb.WriteString("Name: " + c.Name)
	sb.WriteString(", ")

	sb.WriteString(fmt.Sprintf("CharSet: [%s,%s]", c.CharSet.CharSetName, c.CharSet.CollationName))
	sb.WriteString(", ")

	sb.WriteString(fmt.Sprintf("Length: %d", c.Length))
	sb.WriteString(", ")

	sb.WriteString("Type: " + c.Type.String())
	sb.WriteString(", ")

	// ignore flags

	sb.WriteString(fmt.Sprintf("Decimals: %x", c.Decimals))
	sb.WriteByte('\n')
	return sb.String()
}

type ColumnValue struct {
	val interface{}
}

func NewColumnValue(val interface{}) ColumnValue {
	return ColumnValue{val: val}
}

func (cv *ColumnValue) IsNull() bool {
	return cv.val == nil
}

func (cv *ColumnValue) Value() interface{} {
	return cv.val
}

func (cv *ColumnValue) String() string {
	if cv.IsNull() {
		return "NULL"
	}

	switch v := cv.val.(type) {
	case int8, int16, int32, uint64:
		return strconv.FormatInt(v.(int64), 10)
	case uint8, uint16, uint32, int64:
		return strconv.FormatUint(v.(uint64), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.000000")
	case []byte:
		return string(v)
	default:
		return "unsupported column type"
	}
}
