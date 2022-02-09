package command

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// TextResultSetRow https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
type TextResultSetRow struct {
	generic.Header
	Values []ColumnValue
}

func NewTextResultSetRow(row []ColumnValue) *TextResultSetRow {
	return &TextResultSetRow{
		Values: row,
	}
}

func ParseTextResultSetRow(data []byte, columns []*ColumnDefinition, loc *time.Location) (*TextResultSetRow, error) {
	var p TextResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.Values = make([]ColumnValue, len(columns))
	rowData, pos := buf.Bytes(), 0
	for i := range columns {
		cv := ColumnValue{mysqlType: columns[i].ColumnType}

		if rowData[pos] == 0xfb {
			cv.isNull = true
			pos++
		} else {
			buf = bytes.NewBuffer(rowData[pos:])
			befLen := buf.Len()

			cv.value, err = types.LengthEncodedString.Get(buf)
			if err != nil {
				return nil, generic.ErrPacketData
			}

			pos += befLen - buf.Len()
		}

		p.Values[i] = cv
	}

	// convert to Go type
	for i := range p.Values {
		cv := &p.Values[i]

		if cv.IsNull() {
			continue
		}
		val := string(cv.value.([]byte))

		flags := columns[i].Flags
		switch cv.mysqlType {
		case generic.MySQLTypeTiny:
			if flags&generic.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 8)
				if err != nil {
					return nil, err
				}
				cv.value = uint8(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 8)
				if err != nil {
					return nil, err
				}
				cv.value = int8(newVal)
			}

		case generic.MySQLTypeShort, generic.MySQLTypeYear:
			if flags&generic.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 16)
				if err != nil {
					return nil, err
				}
				cv.value = uint16(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 16)
				if err != nil {
					return nil, err
				}
				cv.value = int16(newVal)
			}

		case generic.MySQLTypeInt24, generic.MySQLTypeLong:
			if flags&generic.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 32)
				if err != nil {
					return nil, err
				}
				cv.value = uint32(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 32)
				if err != nil {
					return nil, err
				}
				cv.value = int32(newVal)
			}

		case generic.MySQLTypeLongLong:
			if flags&generic.UnsignedFlag != 0 {
				cv.value, err = strconv.ParseUint(val, 10, 64)
			} else {
				cv.value, err = strconv.ParseInt(val, 10, 64)
			}
			if err != nil {
				return nil, err
			}

		case generic.MySQLTypeFloat:
			newVal, err := strconv.ParseFloat(val, 32)
			if err != nil {
				return nil, err
			}
			cv.value = float32(newVal)

		case generic.MySQLTypeDouble:
			cv.value, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, err
			}

		case generic.MySQLTypeVarchar,
			generic.MySQLTypeBit,
			generic.MySQLTypeEnum,
			generic.MySQLTypeSet,
			generic.MySQLTypeTinyBlob, generic.MySQLTypeMediumBlob, generic.MySQLTypeLongBlob, generic.MySQLTypeBlob,
			generic.MySQLTypeVarString, generic.MySQLTypeString,
			generic.MySQLTypeDecimal, generic.MySQLTypeNewDecimal:
			cv.value = []byte(val)

		case generic.MySQLTypeDate, generic.MySQLTypeDatetime, generic.MySQLTypeTimestamp:
			dt, err := parseDatetime(val, loc)
			if err != nil {
				return nil, err
			}
			cv.value = *dt

		case generic.MySQLTypeTime:
			t, err := parseTime(val)
			if err != nil {
				return nil, err
			}
			cv.value = t

		default:
			return nil, fmt.Errorf("not supported mysql type: %s", cv.mysqlType)
		}
	}

	return &p, nil
}

func (p *TextResultSetRow) Dump(capabilities generic.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer

	for _, val := range p.Values {
		valDump, err := val.DumpText()
		if err != nil {
			return nil, err
		}
		payload.Write(valDump)
	}

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

type BinaryResultSetRow struct {
	generic.Header

	PktHeader  byte
	NullBitMap []byte
	Values     []ColumnValue
}

func ParseBinaryResultSetRow(data []byte, columns []*ColumnDefinition, loc *time.Location) (*BinaryResultSetRow, error) {
	var p BinaryResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	if p.PktHeader, err = buf.ReadByte(); err != nil {
		return nil, err
	}

	columnCount := len(columns)
	nullBitMapLen := (columnCount + 7 + 2) >> 3
	p.NullBitMap = buf.Next(nullBitMapLen)

	p.Values = make([]ColumnValue, columnCount)
	for i := range columns {
		// TODO convert
		cv := ColumnValue{mysqlType: columns[i].ColumnType}

		if p.NullBitMapGet(i) {
			cv.isNull = true
			continue
		}

		flags := columns[i].Flags

		// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
		switch cv.mysqlType {
		case generic.MySQLTypeTiny:
			val := types.FixedLengthInteger.Get(buf.Next(1))
			if flags&generic.UnsignedFlag != 0 {
				cv.value = uint8(val)
			} else {
				cv.value = int8(val)
			}

		case generic.MySQLTypeShort, generic.MySQLTypeYear:
			val := types.FixedLengthInteger.Get(buf.Next(2))
			if flags&generic.UnsignedFlag != 0 {
				cv.value = uint16(val)
			} else {
				cv.value = int16(val)
			}

		case generic.MySQLTypeInt24, generic.MySQLTypeLong:
			val := types.FixedLengthInteger.Get(buf.Next(4))
			if flags&generic.UnsignedFlag != 0 {
				cv.value = uint32(val)
			} else {
				cv.value = int32(val)
			}

		case generic.MySQLTypeLongLong:
			val := types.FixedLengthInteger.Get(buf.Next(8))
			if flags&generic.UnsignedFlag != 0 {
				cv.value = val
			} else {
				cv.value = int64(val)
			}

		case generic.MySQLTypeFloat:
			cv.value = math.Float32frombits(uint32(types.FixedLengthInteger.Get(buf.Next(4))))

		case generic.MySQLTypeDouble:
			cv.value = math.Float64frombits(types.FixedLengthInteger.Get(buf.Next(8)))

		case generic.MySQLTypeVarchar,
			generic.MySQLTypeBit,
			generic.MySQLTypeEnum,
			generic.MySQLTypeSet,
			generic.MySQLTypeTinyBlob, generic.MySQLTypeMediumBlob, generic.MySQLTypeLongBlob, generic.MySQLTypeBlob,
			generic.MySQLTypeVarString, generic.MySQLTypeString,
			generic.MySQLTypeDecimal, generic.MySQLTypeNewDecimal:
			data, err := types.LengthEncodedString.Get(buf)
			if err != nil {
				return nil, err
			}
			cv.value = data

		case generic.MySQLTypeDate, generic.MySQLTypeDatetime, generic.MySQLTypeTimestamp:
			dataLen := types.FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.value = time.Time{}
				continue
			}

			switch dataLen {
			case 0:
				cv.value = time.Time{}
			case 4:
				cv.value = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					0, 0, 0, 0, loc)
			case 7:
				// TODO loc
				cv.value = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					0, loc)
			case 11:
				cv.value = time.Date(
					int(types.FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(types.FixedLengthInteger.Get(buf.Next(1)))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(1))),
					int(types.FixedLengthInteger.Get(buf.Next(4)))*1000,
					loc)
			}

		case generic.MySQLTypeTime:
			dataLen := types.FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.value = time.Time{}
				continue
			}

			isNegative := types.FixedLengthInteger.Get(buf.Next(1)) == 1
			day := int(types.FixedLengthInteger.Get(buf.Next(4)))
			hour := int(types.FixedLengthInteger.Get(buf.Next(1)))
			min := int(types.FixedLengthInteger.Get(buf.Next(1)))
			sec := int(types.FixedLengthInteger.Get(buf.Next(1)))

			var microSec int
			if dataLen == 12 {
				microSec = int(types.FixedLengthInteger.Get(buf.Next(4)))
			}

			sum := time.Duration(24*day+hour)*time.Hour +
				time.Duration(min)*time.Minute +
				time.Duration(sec)*time.Second +
				time.Duration(microSec)*time.Microsecond

			if isNegative {
				sum = -sum
			}
			cv.value = int64(sum)

		default:
			return nil, fmt.Errorf("not supported mysql type: %s", cv.mysqlType)
		}

		p.Values[i] = cv
	}

	return &p, nil
}

func (p *BinaryResultSetRow) NullBitMapGet(index int) bool {
	if p.NullBitMap == nil {
		return false
	}
	offset := 2
	bytePos := (index + offset) >> 3
	bitPos := (index + offset) % 8
	return (p.NullBitMap[bytePos]>>bitPos)&1 != 0
}

type ColumnValue struct {
	isNull    bool
	value     interface{}
	mysqlType generic.TableColumnType
}

func NewColumnValue(isNull bool, val interface{}, mysqlType generic.TableColumnType) ColumnValue {
	return ColumnValue{
		isNull:    isNull,
		value:     val,
		mysqlType: mysqlType,
	}
}

func (v *ColumnValue) IsNull() bool {
	return v.isNull
}

func (v *ColumnValue) Value() interface{} {
	return v.value
}

func (v *ColumnValue) String() string {
	// TODO implement
	return ""
}

func (v *ColumnValue) DumpText() ([]byte, error) {
	if v.isNull {
		return []byte{0xfb}, nil
	}

	switch value := v.value.(type) {
	case time.Time:
		timeStr := value.Format("2006-01-02 15:04:05.000000")
		return types.LengthEncodedString.Dump([]byte(timeStr)), nil
	}

	var val string
	rv := reflect.ValueOf(v.value)
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
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", v.value, ek)
		}
		val = string(rv.Bytes())
	case reflect.String:
		val = rv.String()
	default:
		return nil, fmt.Errorf("unsupported type %T", v.value)
	}

	return types.LengthEncodedString.Dump([]byte(val)), nil
}

func (v *ColumnValue) DumpBinary() []byte {
	// TODO
	return nil
}

func parseDatetime(datetime string, loc *time.Location) (*time.Time, error) {
	switch len(datetime) {

	// only date: 2021-01-24
	case 10:
		dt, err := time.ParseInLocation("2006-01-02", datetime, loc)
		return &dt, err

	// with time: 2006-01-02 15:04:05
	case 19:
		dt, err := time.ParseInLocation("2006-01-02 15:04:05", datetime, loc)
		return &dt, err

	// with microsecond: 2006-01-02 15:04:05.000000
	case 21, 22, 23, 24, 25, 26:
		layout := "2006-01-02 15:04:05."
		for i := 0; i < len(datetime)-20; i++ {
			layout += "0"
		}
		dt, err := time.ParseInLocation(layout, datetime, loc)
		return &dt, err

	default:
		return nil, fmt.Errorf("can't parse datetime string: %s", datetime)
	}
}

func parseTime(t string) (int64, error) {
	buf := bytes.NewBuffer([]byte(t))

	hoursData, err := buf.ReadString(':')
	if err != nil {
		return 0, err
	}
	hoursData = strings.Trim(hoursData, ":")
	minusData, err := buf.ReadString(':')
	if err != nil {
		return 0, err
	}
	minusData = strings.Trim(minusData, ":")
	secsData := string(buf.Next(2))
	var microSecsData string
	if buf.Len() > 0 {
		buf.Next(1)
		microSecsData = string(buf.Bytes())
	}

	var isNegative bool
	if len(hoursData) > 0 && hoursData[0] == '-' {
		isNegative = true
		hoursData = hoursData[1:]
	}

	hours, err := strconv.ParseInt(hoursData, 10, 64)
	if err != nil {
		return 0, err
	}
	minus, err := strconv.ParseInt(minusData, 10, 64)
	if err != nil {
		return 0, err
	}
	secs, err := strconv.ParseInt(secsData, 10, 64)
	if err != nil {
		return 0, err
	}
	var microSecs int64
	if microSecsData != "" {
		if microSecs, err = strconv.ParseInt(microSecsData, 10, 64); err != nil {
			return 0, err
		}
	}

	sum := time.Duration(hours)*time.Hour +
		time.Duration(minus)*time.Minute +
		time.Duration(secs)*time.Second +
		time.Duration(microSecs)*time.Microsecond

	if isNegative {
		sum = -sum
	}

	return int64(sum), nil
}
