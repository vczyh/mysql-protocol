package command

import (
	"bytes"
	"fmt"
	"math"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
	"time"
)

type TextResultSetRow struct {
	generic.Header
	Values []columnValue
}

func ParseTextResultSetRow(data []byte, columns []*ColumnDefinition) (*TextResultSetRow, error) {
	var p TextResultSetRow
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.Values = make([]columnValue, len(columns))
	rowData, pos := buf.Bytes(), 0

	for i := range columns {
		cv := columnValue{mysqlType: columns[i].ColumnType}

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
			rowData = buf.Bytes()
		}

		p.Values[i] = cv
	}

	return &p, nil
}

type BinaryResultSetRow struct {
	generic.Header

	PktHeader  byte
	NullBitMap []byte
	Values     []columnValue
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

	p.Values = make([]columnValue, columnCount)
	for i := range columns {
		// TODO convert
		cv := columnValue{mysqlType: columns[i].ColumnType}

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

type columnValue struct {
	isNull    bool
	value     interface{}
	mysqlType generic.TableColumnType
}

func (v *columnValue) IsNull() bool {
	return v.isNull
}

func (v *columnValue) Value() interface{} {
	return v.value
}

func (v *columnValue) String() string {
	// TODO implement
	return ""
}
