package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"reflect"
	"strconv"
	"time"
)

type Column interface {
	Packet
	GetDatabase() string
	GetTable() string
	GetName() string
	GetCharSet() *core.Collation
	GetLength() uint32
	GetType() core.TableColumnType
	GetFlags() core.ColumnDefinitionFlag
	GetDecimals() byte
	String() string
}

type Value interface{}

type ColumnValue interface {
	IsNull() bool
	Value() Value
	DumpText() ([]byte, error)
	DumpBinary() ([]byte, error)
	String() string
}

// ColumnDefinition https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
type ColumnDefinition struct {
	Header

	Catalog      string // def
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	NextLength   uint64 // 0x0c
	CharacterSet *core.Collation
	ColumnLength uint32
	ColumnType   core.TableColumnType
	Flags        core.ColumnDefinitionFlag
	Decimals     uint8

	// TODO command was COM_FIELD_LIST
}

func ParseColumnDefinition(bs []byte) (Column, error) {
	var p ColumnDefinition
	var err error

	buf := bytes.NewBuffer(bs)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

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

	collationId := uint8(FixedLengthInteger.Get(buf.Next(2)))
	collation, ok := core.CollationIds[collationId]
	if !ok {
		return nil, fmt.Errorf("unknown collation id %d", collationId)
	}
	p.CharacterSet = collation

	p.ColumnLength = uint32(FixedLengthInteger.Get(buf.Next(4)))
	p.ColumnType = core.TableColumnType(FixedLengthInteger.Get(buf.Next(1)))
	p.Flags = core.ColumnDefinitionFlag(FixedLengthInteger.Get(buf.Next(2)))
	p.Decimals = uint8(FixedLengthInteger.Get(buf.Next(1)))

	// filler [00] [00]
	buf.Next(2)

	return &p, nil
}

func (p *ColumnDefinition) Dump(capabilities core.CapabilityFlag) ([]byte, error) {
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

	payload.Write(FixedLengthInteger.Dump(uint64(p.CharacterSet.Id), 2))
	payload.Write(FixedLengthInteger.Dump(uint64(p.ColumnLength), 4))
	payload.WriteByte(byte(p.ColumnType))
	payload.Write(FixedLengthInteger.Dump(uint64(p.Flags), 2))
	payload.WriteByte(p.Decimals)

	payload.Write([]byte{0x00, 0x00})

	p.Length = uint32(payload.Len())

	dump := make([]byte, 3+1+p.Length)
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	copy(dump, headerDump)
	copy(dump[4:], payload.Bytes())

	return dump, nil
}

func (p *ColumnDefinition) GetDatabase() string {
	return p.Schema
}

func (p *ColumnDefinition) GetTable() string {
	return p.Table
}

func (p *ColumnDefinition) GetName() string {
	return p.Name
}

func (p *ColumnDefinition) GetCharSet() *core.Collation {
	return p.CharacterSet
}

func (p *ColumnDefinition) GetLength() uint32 {
	return p.ColumnLength
}

func (p *ColumnDefinition) GetType() core.TableColumnType {
	return p.ColumnType
}

func (p *ColumnDefinition) GetFlags() core.ColumnDefinitionFlag {
	return p.Flags
}

func (p *ColumnDefinition) GetDecimals() byte {
	return p.Decimals
}

func (p *ColumnDefinition) String() string {
	return fmt.Sprintf("%s / %s", p.Name, p.ColumnType)
}

type columnValue struct {
	isNull    bool
	value     Value
	mysqlType core.TableColumnType
}

func NewColumnValue(isNull bool, val interface{}, mysqlType core.TableColumnType) ColumnValue {
	return &columnValue{
		isNull:    isNull,
		value:     val,
		mysqlType: mysqlType,
	}
}

func (cv *columnValue) IsNull() bool {
	return cv.isNull
}

func (cv *columnValue) Value() Value {
	return cv.value
}

func (cv *columnValue) DumpText() ([]byte, error) {
	if cv.isNull {
		return []byte{0xfb}, nil
	}

	switch value := cv.value.(type) {
	case time.Time:
		timeStr := value.Format("2006-01-02 15:04:05.000000")
		return LengthEncodedString.Dump([]byte(timeStr)), nil
	}

	var val string
	rv := reflect.ValueOf(cv.value)
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
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", cv.value, ek)
		}
		val = string(rv.Bytes())
	case reflect.String:
		val = rv.String()
	default:
		return nil, fmt.Errorf("unsupported type %T", cv.value)
	}

	return LengthEncodedString.Dump([]byte(val)), nil
}

func (cv *columnValue) DumpBinary() ([]byte, error) {
	// TODO implement
	return nil, nil
}

func (cv *columnValue) String() string {
	if cv.isNull {
		return "NULL"
	}

	switch v := cv.value.(type) {
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
		return "Unsupported column type"
	}
}
