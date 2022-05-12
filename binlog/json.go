package binlog

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"math"
	"strconv"
	"strings"
	"time"
)

type JsonValue struct {
	data *mysql.Buffer

	// The value if the type is Int or Uint.
	intValue int64

	// The value if the type is Double.
	doubleValue float64

	// Element count for Arrays and Objects.
	// Unused for other types.
	elementCnt int

	// The full length (in bytes) of the binary representation of an Array
	// or Object or the length of a string or Opaque value.
	// Unused for other types.
	length int

	// The MySQL field type of the value, in case the type of the value is Opaque.
	// Otherwise, it is unused.
	fieldType flag.TableColumnType

	t JsonType

	// True if an Array or an Object uses the large storage format with 4
	// byte offsets instead of 2 byte offsets.
	large bool
}

func ParseBinary(buf *mysql.Buffer, length int) (*JsonValue, error) {
	/*
	   Each document should start with a one-byte type specifier, so an
	   empty document is invalid according to the format specification.
	   Empty documents may appear due to inserts using the IGNORE keyword
	   or with non-strict SQL mode, which will insert an empty string if
	   the value NULL is inserted into a NOT NULL column. We choose to
	   interpret empty values as the JSON null literal.
	*/
	if length == 0 {
		return &JsonValue{t: JsonTypeLiteralNull}, nil
	}

	u8, err := buf.Uint8()
	if err != nil {
		return nil, err
	}
	return parseValue(u8, buf, length-1)
}

// Key returns the key of the member stored at the specified position in a JSON
// object.
// TODO charset
func (v *JsonValue) Key(index int) (string, error) {
	if v.t != JsonTypeObject {
		return "", fmt.Errorf("require JsonTypeObject")
	}

	if index >= v.elementCnt {
		return "", fmt.Errorf("invalid json member index")
	}

	keyEntrySize := keyEntrySize(v.large)
	valueEntrySize := valueEntrySize(v.large)

	// The key entries are located after two length fields of size offset_size.
	entryOffset := v.keyEntryOffset(index)

	// The offset of the key is the first part of the key entry.
	buf := v.data.Buffer()
	if _, err := buf.Next(entryOffset); err != nil {
		return "", err
	}
	keyOffset, err := readOffsetOrSize(buf, v.large)
	if err != nil {
		return "", err
	}

	// The length of the key is the second part of the entry, always two bytes.
	keyLength, err := buf.Uint16()
	if err != nil {
		return "", err
	}

	// The key must start somewhere after the last value entry, and it must
	// end before the end of the data buffer.
	if keyOffset < entryOffset+(v.elementCnt-index)*keyEntrySize+v.elementCnt*valueEntrySize ||
		v.length < keyOffset+int(keyLength) {
		return "", fmt.Errorf("json binary data length is not enough")
	}

	buf = v.data.Buffer()
	if _, err := buf.Next(keyOffset); err != nil {
		return "", err
	}
	return buf.NextString(int(keyLength))
}

// Element returns the element at the specified position of a JSON array or a JSON object.
// When called on a JSON object, it returns the value associated with the key returned by key(pos).
// TODO charset
func (v *JsonValue) Element(index int) (*JsonValue, error) {
	if index >= v.elementCnt {
		return nil, fmt.Errorf("invalid json member index")
	}

	entrySize := valueEntrySize(v.large)
	entryOffset := v.valueEntryOffset(index)

	buf := v.data.Buffer()
	if _, err := buf.Next(entryOffset); err != nil {
		return nil, err
	}
	jsonbType, err := buf.Uint8()
	if err != nil {
		return nil, err
	}

	// Check if this is an inlined scalar value. If so, return it.
	// The scalar will be inlined just after the byte that identifies the
	// type, so it's found on entryOffset + 1.
	if isInlinedType(int(jsonbType), v.large) {
		return parseScalar(jsonbType, buf, entrySize-1)
	}

	// Otherwise, it's a non-inlined value, and the offset to where the value
	// is stored, can be found right after the type byte in the entry.
	valueOffset, err := readOffsetOrSize(buf, v.large)
	if err != nil {
		return nil, err
	}

	if v.length < valueOffset || valueOffset < entryOffset+entrySize {
		return nil, fmt.Errorf("json binary data invalid")
	}

	buf = v.data.Buffer()
	if _, err := buf.Next(valueOffset); err != nil {
		return nil, err
	}
	return parseValue(jsonbType, buf, v.length-valueOffset)
}

// TODO charset
func (v *JsonValue) WriteStringBuilder(sb *strings.Builder, loc *time.Location) error {
	switch v.t {
	case JsonTypeObject:
		sb.WriteByte('{')
		for i := 0; i < v.elementCnt; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			key, err := v.Key(i)
			if err != nil {
				return err
			}
			sb.WriteString(strconv.Quote(key))
			sb.WriteString(": ")
			element, err := v.Element(i)
			if err != nil {
				return err
			}
			if err := element.WriteStringBuilder(sb, loc); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	case JsonTypeArray:
		sb.WriteByte('[')
		for i := 0; i < v.elementCnt; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			element, err := v.Element(i)
			if err != nil {
				return err
			}
			if err := element.WriteStringBuilder(sb, loc); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
	case JsonTypeDouble:
		sb.WriteString(strconv.FormatFloat(v.doubleValue, 'f', -1, 64))
	case JsonTypeInt:
		sb.WriteString(strconv.FormatInt(v.intValue, 10))
	case JsonTypeUint:
		sb.WriteString(strconv.FormatUint(uint64(v.intValue), 10))
	case JsonTypeLiteralFalse:
		sb.WriteString("false")
	case JsonTypeLiteralTrue:
		sb.WriteString("true")
	case JsonTypeLiteralNull:
		sb.WriteString("null")
	case JsonTypeOpaque:
		switch v.fieldType {
		case flag.MySQLTypeNewDecimal:
			buf := v.data.Buffer()
			precision, err := buf.Uint8()
			if err != nil {
				return err
			}
			frac, err := buf.Uint8()
			if err != nil {
				return err
			}
			decimal, err := buf.Decimal(int(precision), int(frac))
			if err != nil {
				return err
			}
			sb.WriteString(decimal)
		case flag.MySQLTypeTime, flag.MySQLTypeDate, flag.MySQLTypeDatetime, flag.MySQLTypeTimestamp:
			buf := v.data.Buffer()
			u64, err := buf.Uint64()
			if err != nil {
				return err
			}
			t, err := core.TimeFromInt64(v.fieldType, int64(u64), loc)
			if err != nil {
				return err
			}
			sb.WriteString(strconv.Quote(t.String()))
		case flag.MySQLTypeVarString:
			str, err := v.data.Buffer().NextString(v.length)
			if err != nil {
				return err
			}
			sb.WriteString(strconv.Quote(str))
		default:
			return fmt.Errorf("unsupported filedType %s in json opaque element", v.fieldType)
		}
	case JsonTypeString:
		str, err := v.data.Buffer().NextString(v.length)
		if err != nil {
			return err
		}
		sb.WriteString(strconv.Quote(str))
	default:
		return fmt.Errorf("unsupported JsonType %d", v.t)
	}

	return nil
}

// Get the offset of the key entry that describes the key of the member at a
// given position in this object.
func (v *JsonValue) keyEntryOffset(index int) int {
	// The first key entry is located right after the two length fields.
	return 2*offsetSize(v.large) + keyEntrySize(v.large)*index
}

// Get the offset of the value entry that describes the element at a
// given position in this array or object.
func (v *JsonValue) valueEntryOffset(index int) int {
	// Value entries come after the two length fields if it's an array, or
	// after the two length fields and all the key entries if it's an object
	firstEntryOffset := 2 * offsetSize(v.large)
	if v.t == JsonTypeObject {
		firstEntryOffset += v.elementCnt * keyEntrySize(v.large)
	}
	return firstEntryOffset + valueEntrySize(v.large)*index
}

func parseValue(t uint8, buf *mysql.Buffer, length int) (*JsonValue, error) {
	switch t {
	case JsonbTypeSmallObject:
		return parseArrayOrObject(JsonTypeObject, buf, length, false)
	case JsonbTypeLargeObject:
		return parseArrayOrObject(JsonTypeObject, buf, length, true)
	case JsonbTypeSmallArray:
		return parseArrayOrObject(JsonTypeArray, buf, length, false)
	case JsonbTypeLargeArray:
		return parseArrayOrObject(JsonTypeArray, buf, length, true)
	default:
		return parseScalar(t, buf, length)
	}
}

func parseArrayOrObject(t JsonType, buf *mysql.Buffer, length int, large bool) (*JsonValue, error) {
	if t != JsonTypeArray && t != JsonTypeObject {
		return nil, fmt.Errorf("required JsonType is Array or Object")
	}

	data := buf.Buffer()

	// Make sure the document is long enough to contain the two length fields
	// (both number of elements or members, and number of bytes).
	offsetSize := offsetSize(large)
	if length < 2*offsetSize {
		return nil, fmt.Errorf("document is not long enough to contain two length fields")
	}

	elementCnt, err := readOffsetOrSize(buf, large)
	if err != nil {
		return nil, err
	}
	bytes, err := readOffsetOrSize(buf, large)
	if err != nil {
		return nil, err
	}
	if bytes > length {
		return nil, fmt.Errorf("the value can't have more bytes than what's available in the data buffer")
	}

	// Calculate the size of the header. It consists of:
	// - two length fields
	// - if it is a JSON object, key entries with pointers to where the keys are stored
	// - value entries with pointers to where the actual values are stored
	headerSize := 2 * offsetSize
	if t == JsonTypeObject {
		headerSize += elementCnt * keyEntrySize(large)
	}
	headerSize += elementCnt * valueEntrySize(large)
	if headerSize > bytes {
		return nil, fmt.Errorf("the header should not be larger than the full size of the value")
	}

	return &JsonValue{
		data:       data,
		elementCnt: elementCnt,
		length:     bytes,
		t:          t,
		large:      large,
	}, nil
}

func parseScalar(t uint8, buf *mysql.Buffer, length int) (*JsonValue, error) {
	switch t {
	case JsonbTypeLiteral:
		u8, err := buf.Uint8()
		if err != nil {
			return nil, err
		}
		switch int(u8) {
		case JsonbNullLiteral:
			return &JsonValue{t: JsonTypeLiteralNull}, nil
		case JsonbTrueLiteral:
			return &JsonValue{t: JsonTypeLiteralTrue}, nil
		case JsonbFalseLiteral:
			return &JsonValue{t: JsonTypeLiteralFalse}, nil
		default:
			return nil, fmt.Errorf("invalid JsonbTypeLiteral %x", int(u8))
		}
	case JsonbTypeInt16:
		u16, err := buf.Uint16()
		return &JsonValue{t: JsonTypeInt, intValue: int64(u16)}, err
	case JsonbTypeInt32:
		u32, err := buf.Uint32()
		return &JsonValue{t: JsonTypeInt, intValue: int64(u32)}, err
	case JsonbTypeInt64:
		u64, err := buf.Uint64()
		return &JsonValue{t: JsonTypeInt, intValue: int64(u64)}, err
	case JsonbTypeUint16:
		u16, err := buf.Uint16()
		return &JsonValue{t: JsonTypeUint, intValue: int64(u16)}, err
	case JsonbTypeUint32:
		u32, err := buf.Uint32()
		return &JsonValue{t: JsonTypeUint, intValue: int64(u32)}, err
	case JsonbTypeUint64:
		u64, err := buf.Uint64()
		return &JsonValue{t: JsonTypeUint, intValue: int64(u64)}, err
	case JsonbTypeDouble:
		f64, err := buf.Float64()
		return &JsonValue{t: JsonTypeDouble, doubleValue: f64}, err
	case JsonbTypeString:
		beforeLen := buf.Len()
		u32, err := readVariableLength(buf, length)
		if err != nil {
			return nil, err
		}
		strLen := int(u32)

		if length < beforeLen-buf.Len()+strLen {
			return nil, fmt.Errorf("json binary data length is not enough")
		}

		return &JsonValue{
			data:   buf,
			length: strLen,
			t:      JsonTypeString,
		}, nil
	case JsonbTypeOpaque:
		// There should always be at least one byte, which tells the field
		// type of the opaque value.
		// The type is encoded as a uint8 that maps to an enum_field_types.
		u8, err := buf.Uint8()
		if err != nil {
			return nil, err
		}
		fieldType := flag.TableColumnType(u8)

		// Then there's the length of the value.
		beforeLen := buf.Len()
		u32, err := readVariableLength(buf, length-1)
		if err != nil {
			return nil, err
		}
		valLen := int(u32)

		if length < 1+beforeLen-buf.Len()+valLen {
			return nil, fmt.Errorf("json binary data length is not enough")
		}

		return &JsonValue{
			data:      buf,
			length:    valLen,
			fieldType: fieldType,
			t:         JsonTypeOpaque,
		}, nil
	default:
		return nil, fmt.Errorf("not a valid scalar type")
	}
}

func readVariableLength(buf *mysql.Buffer, length int) (uint32, error) {
	// It takes five bytes to represent UINT_MAX32, which is the largest
	// supported length, so don't look any further.
	maxBytes := length
	if length > 5 {
		maxBytes = 5
	}

	var strLen uint64
	for i := 0; i < maxBytes; i++ {
		b, err := buf.ReadByte()
		if err != nil {
			return 0, err
		}

		// Get the next 7 bits of the length.
		strLen |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			// The length shouldn't exceed 32 bits.
			if strLen > math.MaxUint32 {
				return 0, fmt.Errorf("invalid json data")
			}

			// This was the last byte. Return successfully.
			return uint32(strLen), nil
		}
	}

	return 0, fmt.Errorf("invalid json data")
}

func offsetSize(large bool) int {
	if large {
		return LargeOffsetSize
	}
	return SmallOffsetSize
}

func readOffsetOrSize(buf *mysql.Buffer, large bool) (int, error) {
	if large {
		u32, err := buf.Uint32()
		return int(u32), err
	}
	u16, err := buf.Uint16()
	return int(u16), err
}

func keyEntrySize(large bool) int {
	if large {
		return KeyEntrySizeLarge
	}
	return KeyEntrySizeSmall
}

func valueEntrySize(large bool) int {
	if large {
		return ValueEntrySizeLarge
	}
	return ValueEntrySizeSmall
}

func isInlinedType(jsonbType int, large bool) bool {
	switch jsonbType {
	case JsonbTypeLiteral,
		JsonbTypeInt16,
		JsonbTypeUint16:
		return true
	case JsonbTypeInt32,
		JsonbTypeUint32:
		return large
	default:
		return false
	}
}

type JsonType uint8

const (
	JsonTypeObject JsonType = iota
	JsonTypeArray
	JsonTypeString
	JsonTypeInt
	JsonTypeUint
	JsonTypeDouble
	JsonTypeLiteralNull
	JsonTypeLiteralTrue
	JsonTypeLiteralFalse
	JsonTypeOpaque

	// JsonTypeError not really a type. Used to signal that an error was detected.
	JsonTypeError
)

const (
	JsonbTypeSmallObject = 0x0
	JsonbTypeLargeObject = 0x1
	JsonbTypeSmallArray  = 0x2
	JsonbTypeLargeArray  = 0x3
	JsonbTypeLiteral     = 0x4
	JsonbTypeInt16       = 0x5
	JsonbTypeUint16      = 0x6
	JsonbTypeInt32       = 0x7
	JsonbTypeUint32      = 0x8
	JsonbTypeInt64       = 0x9
	JsonbTypeUint64      = 0xA
	JsonbTypeDouble      = 0xB
	JsonbTypeString      = 0xC
	JsonbTypeOpaque      = 0xF

	JsonbNullLiteral  = 0x0
	JsonbTrueLiteral  = 0x1
	JsonbFalseLiteral = 0x2

	// The size of offset or size fields in the small and the large storage
	// format for JSON objects and JSON arrays.

	SmallOffsetSize = 2
	LargeOffsetSize = 4

	// The size of key entries for objects when using the small storage
	// format or the large storage format. In the small format it is 4
	// bytes (2 bytes for key length and 2 bytes for key offset). In the
	// large format it is 6 (2 bytes for length, 4 bytes for offset).

	KeyEntrySizeSmall = 2 + SmallOffsetSize
	KeyEntrySizeLarge = 2 + LargeOffsetSize

	// The size of value entries for objects or arrays. When using the
	// small storage format, the entry size is 3 (1 byte for type, 2 bytes
	// for offset). When using the large storage format, it is 5 (1 byte
	// for type, 4 bytes for offset).

	ValueEntrySizeSmall = 1 + SmallOffsetSize
	ValueEntrySizeLarge = 1 + LargeOffsetSize
)
