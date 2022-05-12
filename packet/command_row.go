package packet

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/flag"
	"math"
	"strconv"
	"strings"
	"time"
)

type Row []ColumnValue

// TextResultSetRow https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
type TextResultSetRow struct {
	Row Row
}

func ParseTextResultSetRow(data []byte, columns []*ColumnDefinition, loc *time.Location) (Row, error) {
	var p TextResultSetRow
	var err error

	buf := bytes.NewBuffer(data)

	values := make([]ColumnValue, len(columns))
	rowData, pos := buf.Bytes(), 0
	for i := range columns {
		var cv ColumnValue
		//cv := ColumnValue{mysqlType: columns[i].GetType()}

		if rowData[pos] == 0xfb {
			cv.Value = nil
			pos++
		} else {
			buf = bytes.NewBuffer(rowData[pos:])
			befLen := buf.Len()

			cv.Value, err = LengthEncodedString.Get(buf)
			if err != nil {
				return nil, ErrPacketData
			}

			pos += befLen - buf.Len()
		}
		values[i] = cv
	}

	// convert to Go type
	for i := range values {
		cv := values[i]
		if cv.Value == nil {
			continue
		}
		val := string(cv.Value.([]byte))

		flags := columns[i].Flags
		switch columns[i].ColumnType {
		case flag.MySQLTypeTiny:
			if flags&flag.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 8)
				if err != nil {
					return nil, err
				}
				cv.Value = uint8(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 8)
				if err != nil {
					return nil, err
				}
				cv.Value = int8(newVal)
			}

		case flag.MySQLTypeShort, flag.MySQLTypeYear:
			if flags&flag.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 16)
				if err != nil {
					return nil, err
				}
				cv.Value = uint16(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 16)
				if err != nil {
					return nil, err
				}
				cv.Value = int16(newVal)
			}

		case flag.MySQLTypeInt24, flag.MySQLTypeLong:
			if flags&flag.UnsignedFlag != 0 {
				newVal, err := strconv.ParseUint(val, 10, 32)
				if err != nil {
					return nil, err
				}
				cv.Value = uint32(newVal)
			} else {
				newVal, err := strconv.ParseInt(val, 10, 32)
				if err != nil {
					return nil, err
				}
				cv.Value = int32(newVal)
			}

		case flag.MySQLTypeLongLong:
			if flags&flag.UnsignedFlag != 0 {
				cv.Value, err = strconv.ParseUint(val, 10, 64)
			} else {
				cv.Value, err = strconv.ParseInt(val, 10, 64)
			}
			if err != nil {
				return nil, err
			}

		case flag.MySQLTypeFloat:
			newVal, err := strconv.ParseFloat(val, 32)
			if err != nil {
				return nil, err
			}
			cv.Value = float32(newVal)

		case flag.MySQLTypeDouble:
			cv.Value, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, err
			}

		case flag.MySQLTypeVarchar,
			flag.MySQLTypeBit,
			flag.MySQLTypeEnum,
			flag.MySQLTypeSet,
			flag.MySQLTypeTinyBlob, flag.MySQLTypeMediumBlob, flag.MySQLTypeLongBlob, flag.MySQLTypeBlob,
			flag.MySQLTypeVarString, flag.MySQLTypeString,
			flag.MySQLTypeDecimal, flag.MySQLTypeNewDecimal:
			cv.Value = []byte(val)

		case flag.MySQLTypeDate, flag.MySQLTypeDatetime, flag.MySQLTypeTimestamp:
			dt, err := parseDatetime(val, loc)
			if err != nil {
				return nil, err
			}
			cv.Value = *dt

		case flag.MySQLTypeTime:
			t, err := parseTime(val)
			if err != nil {
				return nil, err
			}
			cv.Value = t

		default:
			return nil, fmt.Errorf("not supported mysql type: %s", columns[i].ColumnType)
		}
	}

	// TODO
	for _, val := range values {
		p.Row = append(p.Row, val)
	}

	return p.Row, nil
}

func (p *TextResultSetRow) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer

	for _, val := range p.Row {
		valDump, err := val.DumpText()
		if err != nil {
			return nil, err
		}
		payload.Write(valDump)
	}

	return payload.Bytes(), nil
}

type BinaryResultSetRow struct {
	PktHeader  byte // 0x00
	NullBitMap []byte
	Row        Row
}

func ParseBinaryResultSetRow(data []byte, columns []ColumnDefinition, loc *time.Location) (Row, error) {
	var p BinaryResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if p.PktHeader, err = buf.ReadByte(); err != nil {
		return nil, err
	}

	columnCount := len(columns)
	nullBitMapLen := (columnCount + 7 + 2) >> 3
	p.NullBitMap = buf.Next(nullBitMapLen)

	values := make([]ColumnValue, columnCount)
	for i := range columns {
		var cv ColumnValue
		//cv := ColumnValue{mysqlType: columns[i].GetType()}

		if p.NullBitMapGet(i) {
			cv.Value = nil
			//cv.isNull = true
			continue
		}

		flags := columns[i].Flags

		// https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
		switch columns[i].ColumnType {
		case flag.MySQLTypeTiny:
			val := FixedLengthInteger.Get(buf.Next(1))
			if flags&flag.UnsignedFlag != 0 {
				cv.Value = uint8(val)
			} else {
				cv.Value = int8(val)
			}

		case flag.MySQLTypeShort, flag.MySQLTypeYear:
			val := FixedLengthInteger.Get(buf.Next(2))
			if flags&flag.UnsignedFlag != 0 {
				cv.Value = uint16(val)
			} else {
				cv.Value = int16(val)
			}

		case flag.MySQLTypeInt24, flag.MySQLTypeLong:
			val := FixedLengthInteger.Get(buf.Next(4))
			if flags&flag.UnsignedFlag != 0 {
				cv.Value = uint32(val)
			} else {
				cv.Value = int32(val)
			}

		case flag.MySQLTypeLongLong:
			val := FixedLengthInteger.Get(buf.Next(8))
			if flags&flag.UnsignedFlag != 0 {
				cv.Value = val
			} else {
				cv.Value = int64(val)
			}

		case flag.MySQLTypeFloat:
			cv.Value = math.Float32frombits(uint32(FixedLengthInteger.Get(buf.Next(4))))

		case flag.MySQLTypeDouble:
			cv.Value = math.Float64frombits(FixedLengthInteger.Get(buf.Next(8)))

		case flag.MySQLTypeVarchar,
			flag.MySQLTypeBit,
			flag.MySQLTypeEnum,
			flag.MySQLTypeSet,
			flag.MySQLTypeTinyBlob, flag.MySQLTypeMediumBlob, flag.MySQLTypeLongBlob, flag.MySQLTypeBlob,
			flag.MySQLTypeVarString, flag.MySQLTypeString,
			flag.MySQLTypeDecimal, flag.MySQLTypeNewDecimal:
			data, err := LengthEncodedString.Get(buf)
			if err != nil {
				return nil, err
			}
			cv.Value = data

		case flag.MySQLTypeDate, flag.MySQLTypeDatetime, flag.MySQLTypeTimestamp:
			dataLen := FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.Value = time.Time{}
				continue
			}

			switch dataLen {
			case 0:
				cv.Value = time.Time{}
			case 4:
				cv.Value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					0, 0, 0, 0, loc)
			case 7:
				cv.Value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					0, loc)
			case 11:
				cv.Value = time.Date(
					int(FixedLengthInteger.Get(buf.Next(2))),
					time.Month(int(FixedLengthInteger.Get(buf.Next(1)))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(1))),
					int(FixedLengthInteger.Get(buf.Next(4)))*1000,
					loc)
			}

		case flag.MySQLTypeTime:
			dataLen := FixedLengthInteger.Get(buf.Next(1))
			if dataLen == 0 {
				cv.Value = time.Time{}
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
			cv.Value = int64(sum)

		default:
			return nil, fmt.Errorf("not supported mysql type: %s", columns[i].ColumnType)
		}

		values[i] = cv
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

func (p *BinaryResultSetRow) Dump(capability flag.Capability) ([]byte, error) {
	// TODO
	panic("implement me")
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
