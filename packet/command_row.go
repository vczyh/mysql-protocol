package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"math"
	"strconv"
	"strings"
	"time"
)

type Row []ColumnValue

func (r Row) String() string {
	values := make([]string, len(r))
	for i, cv := range r {
		values[i] = cv.String()
	}
	return strings.Join(values, " | ")
}

// TextResultSetRow https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
type TextResultSetRow struct {
	Header
	Row Row
}

func NewTextResultSetRow(row Row) *TextResultSetRow {
	return &TextResultSetRow{
		Row: row,
	}
}

func ParseTextResultSetRow(data []byte, columns []Column, loc *time.Location) (Row, error) {
	var p TextResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	values := make([]*columnValue, len(columns))
	rowData, pos := buf.Bytes(), 0
	for i := range columns {
		cv := columnValue{mysqlType: columns[i].GetType()}

		if rowData[pos] == 0xfb {
			cv.isNull = true
			pos++
		} else {
			buf = bytes.NewBuffer(rowData[pos:])
			befLen := buf.Len()

			cv.value, err = LengthEncodedString.Get(buf)
			if err != nil {
				return nil, ErrPacketData
			}

			pos += befLen - buf.Len()
		}
		values[i] = &cv
	}

	// convert to Go type
	for i := range values {
		cv := values[i]

		if cv.IsNull() {
			continue
		}
		val := string(cv.value.([]byte))

		flags := columns[i].GetFlags()
		switch cv.mysqlType {
		case core.MySQLTypeTiny:
			if flags&core.UnsignedFlag != 0 {
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

		case core.MySQLTypeShort, core.MySQLTypeYear:
			if flags&core.UnsignedFlag != 0 {
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

		case core.MySQLTypeInt24, core.MySQLTypeLong:
			if flags&core.UnsignedFlag != 0 {
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

		case core.MySQLTypeLongLong:
			if flags&core.UnsignedFlag != 0 {
				cv.value, err = strconv.ParseUint(val, 10, 64)
			} else {
				cv.value, err = strconv.ParseInt(val, 10, 64)
			}
			if err != nil {
				return nil, err
			}

		case core.MySQLTypeFloat:
			newVal, err := strconv.ParseFloat(val, 32)
			if err != nil {
				return nil, err
			}
			cv.value = float32(newVal)

		case core.MySQLTypeDouble:
			cv.value, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, err
			}

		case core.MySQLTypeVarchar,
			core.MySQLTypeBit,
			core.MySQLTypeEnum,
			core.MySQLTypeSet,
			core.MySQLTypeTinyBlob, core.MySQLTypeMediumBlob, core.MySQLTypeLongBlob, core.MySQLTypeBlob,
			core.MySQLTypeVarString, core.MySQLTypeString,
			core.MySQLTypeDecimal, core.MySQLTypeNewDecimal:
			cv.value = []byte(val)

		case core.MySQLTypeDate, core.MySQLTypeDatetime, core.MySQLTypeTimestamp:
			dt, err := parseDatetime(val, loc)
			if err != nil {
				return nil, err
			}
			cv.value = *dt

		case core.MySQLTypeTime:
			t, err := parseTime(val)
			if err != nil {
				return nil, err
			}
			cv.value = t

		default:
			return nil, fmt.Errorf("not supported mysql type: %s", cv.mysqlType)
		}
	}

	for _, val := range values {
		p.Row = append(p.Row, val)
	}

	return p.Row, nil
}

func (p *TextResultSetRow) Dump(capabilities core.CapabilityFlag) ([]byte, error) {
	var payload bytes.Buffer

	for _, val := range p.Row {
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
	Header

	PktHeader  byte
	NullBitMap []byte
	Row        Row
}

func ParseBinaryResultSetRow(data []byte, columns []Column, loc *time.Location) (Row, error) {
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

	values := make([]*columnValue, columnCount)
	for i := range columns {
		cv := columnValue{mysqlType: columns[i].GetType()}

		if p.NullBitMapGet(i) {
			cv.isNull = true
			continue
		}

		flags := columns[i].GetFlags()

		// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
		switch cv.mysqlType {
		case core.MySQLTypeTiny:
			val := FixedLengthInteger.Get(buf.Next(1))
			if flags&core.UnsignedFlag != 0 {
				cv.value = uint8(val)
			} else {
				cv.value = int8(val)
			}

		case core.MySQLTypeShort, core.MySQLTypeYear:
			val := FixedLengthInteger.Get(buf.Next(2))
			if flags&core.UnsignedFlag != 0 {
				cv.value = uint16(val)
			} else {
				cv.value = int16(val)
			}

		case core.MySQLTypeInt24, core.MySQLTypeLong:
			val := FixedLengthInteger.Get(buf.Next(4))
			if flags&core.UnsignedFlag != 0 {
				cv.value = uint32(val)
			} else {
				cv.value = int32(val)
			}

		case core.MySQLTypeLongLong:
			val := FixedLengthInteger.Get(buf.Next(8))
			if flags&core.UnsignedFlag != 0 {
				cv.value = val
			} else {
				cv.value = int64(val)
			}

		case core.MySQLTypeFloat:
			cv.value = math.Float32frombits(uint32(FixedLengthInteger.Get(buf.Next(4))))

		case core.MySQLTypeDouble:
			cv.value = math.Float64frombits(FixedLengthInteger.Get(buf.Next(8)))

		case core.MySQLTypeVarchar,
			core.MySQLTypeBit,
			core.MySQLTypeEnum,
			core.MySQLTypeSet,
			core.MySQLTypeTinyBlob, core.MySQLTypeMediumBlob, core.MySQLTypeLongBlob, core.MySQLTypeBlob,
			core.MySQLTypeVarString, core.MySQLTypeString,
			core.MySQLTypeDecimal, core.MySQLTypeNewDecimal:
			data, err := LengthEncodedString.Get(buf)
			if err != nil {
				return nil, err
			}
			cv.value = data

		case core.MySQLTypeDate, core.MySQLTypeDatetime, core.MySQLTypeTimestamp:
			dataLen := FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.value = time.Time{}
				continue
			}

			switch dataLen {
			case 0:
				cv.value = time.Time{}
			case 4:
				cv.value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					0, 0, 0, 0, loc)
			case 7:
				// TODO loc
				cv.value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					0, loc)
			case 11:
				cv.value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(4)))*1000,
					loc)
			}

		case core.MySQLTypeTime:
			dataLen := FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.value = time.Time{}
				continue
			}

			isNegative := FixedLengthInteger.Get(buf.Next(1)) == 1
			day := int(FixedLengthInteger.Get(buf.Next(4)))
			hour := int(FixedLengthInteger.Get(buf.Next(1)))
			min := int(FixedLengthInteger.Get(buf.Next(1)))
			sec := int(FixedLengthInteger.Get(buf.Next(1)))

			var microSec int
			if dataLen == 12 {
				microSec = int(FixedLengthInteger.Get(buf.Next(4)))
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

		values[i] = &cv
	}

	for _, val := range values {
		p.Row = append(p.Row, val)
	}

	return p.Row, nil
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
