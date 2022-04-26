package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"reflect"
	"strconv"
	"time"
)

type TableColumnType uint8

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
// https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
const (
	MySQLTypeDecimal TableColumnType = iota
	MySQLTypeTiny
	MySQLTypeShort
	MySQLTypeLong
	MySQLTypeFloat
	MySQLTypeDouble
	MySQLTypeNull
	MySQLTypeTimestamp
	MySQLTypeLongLong
	MySQLTypeInt24
	MySQLTypeDate
	MySQLTypeTime
	MySQLTypeDatetime
	MySQLTypeYear
	MySQLTypeNewDate
	MySQLTypeVarchar
	MySQLTypeBit
	MySQLTypeTimestamp2
	MySQLTypeDatetime2
	MySQLTypeTime2
	MySQLTypeTypedArray
	MySQLTypeInvalid = iota + 0xde
	MySQLTypeBool
	MySQLTypeJson
	MySQLTypeNewDecimal
	MySQLTypeEnum
	MySQLTypeSet
	MySQLTypeTinyBlob
	MySQLTypeMediumBlob
	MySQLTypeLongBlob
	MySQLTypeBlob
	MySQLTypeVarString
	MySQLTypeString
	MySQLTypeGeometry
)

func (t TableColumnType) String() string {
	switch t {
	case MySQLTypeDecimal:
		return "MYSQL_TYPE_DECIMAL"
	case MySQLTypeTiny:
		return "MYSQL_TYPE_TINY"
	case MySQLTypeShort:
		return "MYSQL_TYPE_SHORT"
	case MySQLTypeLong:
		return "MYSQL_TYPE_LONG"
	case MySQLTypeFloat:
		return "MYSQL_TYPE_FLOAT"
	case MySQLTypeDouble:
		return "MYSQL_TYPE_DOUBLE"
	case MySQLTypeNull:
		return "MYSQL_TYPE_NULL"
	case MySQLTypeTimestamp:
		return "MYSQL_TYPE_TIMESTAMP"
	case MySQLTypeLongLong:
		return "MYSQL_TYPE_LONGLONG"
	case MySQLTypeInt24:
		return "MYSQL_TYPE_INT24"
	case MySQLTypeDate:
		return "MYSQL_TYPE_DATE"
	case MySQLTypeTime:
		return "MYSQL_TYPE_TIME"
	case MySQLTypeDatetime:
		return "MYSQL_TYPE_DATETIME"
	case MySQLTypeYear:
		return "MYSQL_TYPE_YEAR"
	case MySQLTypeNewDate:
		return "MYSQL_TYPE_NEWDATE"
	case MySQLTypeVarchar:
		return "MYSQL_TYPE_VARCHAR"
	case MySQLTypeBit:
		return "MYSQL_TYPE_BIT"
	case MySQLTypeTimestamp2:
		return "MYSQL_TYPE_TIMESTAMP2"
	case MySQLTypeDatetime2:
		return "MYSQL_TYPE_DATETIME2"
	case MySQLTypeTime2:
		return "MYSQL_TYPE_TIME2"
	case MySQLTypeTypedArray:
		return "MYSQL_TYPE_TYPED_ARRAY"
	case MySQLTypeInvalid:
		return "MYSQL_TYPE_INVALID"
	case MySQLTypeBool:
		return "MYSQL_TYPE_BOOL"
	case MySQLTypeJson:
		return "MYSQL_TYPE_JSON"
	case MySQLTypeNewDecimal:
		return "MYSQL_TYPE_NEWDECIMAL"
	case MySQLTypeEnum:
		return "MYSQL_TYPE_ENUM"
	case MySQLTypeSet:
		return "MYSQL_TYPE_SET"
	case MySQLTypeTinyBlob:
		return "MYSQL_TYPE_TINY_BLOB"
	case MySQLTypeMediumBlob:
		return "MYSQL_TYPE_MEDIUM_BLOB"
	case MySQLTypeLongBlob:
		return "MYSQL_TYPE_LONG_BLOB"
	case MySQLTypeBlob:
		return "MYSQL_TYPE_BLOB"
	case MySQLTypeVarString:
		return "MYSQL_TYPE_VAR_STRING"
	case MySQLTypeString:
		return "MYSQL_TYPE_STRING"
	case MySQLTypeGeometry:
		return "MYSQL_TYPE_GEOMETRY"
	default:
		return "Unknown TableColumnType"
	}
}

// ColumnDefinition https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
type ColumnDefinition struct {
	Catalog      string // def
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	NextLength   uint64 // 0x0c
	CharacterSet *charset.Collation
	ColumnLength uint32
	ColumnType   TableColumnType
	Flags        flag.ColumnDefinition
	Decimals     uint8

	// TODO command was COM_FIELD_LIST
}

func ParseColumnDefinition(bs []byte) (p *ColumnDefinition, err error) {
	p = new(ColumnDefinition)
	buf := bytes.NewBuffer(bs)

	var b []byte
	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Catalog = string(b)

	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Schema = string(b)

	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Table = string(b)

	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.OrgTable = string(b)

	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.Name = string(b)

	if b, err = LengthEncodedString.Get(buf); err != nil {
		return nil, err
	}
	p.OrgName = string(b)

	if p.NextLength, err = LengthEncodedInteger.Get(buf); err != nil {
		return nil, err
	}

	collationId := FixedLengthInteger.Get(buf.Next(2))
	p.CharacterSet, err = charset.GetCollation(collationId)
	if err != nil {
		return nil, err
	}

	p.ColumnLength = uint32(FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnType = TableColumnType(FixedLengthInteger.Get(buf.Next(1)))
	p.Flags = flag.ColumnDefinition(FixedLengthInteger.Get(buf.Next(2)))
	p.Decimals = uint8(FixedLengthInteger.Get(buf.Next(1)))

	// filler [00] [00]
	buf.Next(2)

	return p, nil
}

func (p *ColumnDefinition) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer

	if p.Catalog == "" {
		p.Catalog = "def"
	}
	payload.Write(LengthEncodedString.Dump([]byte(p.Catalog)))

	payload.Write(LengthEncodedString.Dump([]byte(p.Schema)))
	payload.Write(LengthEncodedString.Dump([]byte(p.Table)))
	payload.Write(LengthEncodedString.Dump([]byte(p.OrgTable)))
	payload.Write(LengthEncodedString.Dump([]byte(p.Name)))
	payload.Write(LengthEncodedString.Dump([]byte(p.OrgName)))

	if p.NextLength == 0 {
		p.NextLength = 0x0c
	}
	payload.Write(LengthEncodedInteger.Dump(p.NextLength))

	payload.Write(FixedLengthInteger.Dump(p.CharacterSet.Id(), 2))
	payload.Write(FixedLengthInteger.Dump(uint64(p.ColumnLength), 4))
	payload.WriteByte(byte(p.ColumnType))
	payload.Write(FixedLengthInteger.Dump(uint64(p.Flags), 2))
	payload.WriteByte(p.Decimals)

	payload.Write([]byte{0x00, 0x00})

	return payload.Bytes(), nil
}

type ColumnValue struct {
	Value interface{}
}

func (cv *ColumnValue) DumpText() ([]byte, error) {
	if cv.Value == nil {
		return []byte{0xfb}, nil
	}

	switch value := cv.Value.(type) {
	case time.Time:
		timeStr := value.Format("2006-01-02 15:04:05.000000")
		return LengthEncodedString.Dump([]byte(timeStr)), nil
	}

	var val string
	rv := reflect.ValueOf(cv.Value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val = strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		val = strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32:
		val = strconv.FormatFloat(rv.Float(), 'f', -1, 32)
	case reflect.Float64:
		val = strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	case reflect.Slice:
		ek := rv.Type().Elem().Kind()
		if ek != reflect.Uint8 {
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", cv.Value, ek)
		}
		val = string(rv.Bytes())
	case reflect.String:
		val = rv.String()
	default:
		return nil, fmt.Errorf("unsupported type %T", cv.Value)
	}

	return LengthEncodedString.Dump([]byte(val)), nil
}

func (cv *ColumnValue) DumpBinary() ([]byte, error) {
	// TODO implement
	return nil, nil
}
