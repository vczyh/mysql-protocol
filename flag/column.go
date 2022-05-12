package flag

type ColumnDefinition uint16

// Column Definition Flags: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
const (
	NotNullFlag ColumnDefinition = 1 << iota
	PriKeyFlag
	UniqueKeyFlag
	MultipleKeyFlag
	BlobFlag
	UnsignedFlag
	ZerofillFlag
	BinaryFlag
	EnumFlag
	AutoIncrementFlag
	TimestampFlag
	SetFlag
)

func (cd ColumnDefinition) String() string {
	switch cd {
	case NotNullFlag:
		return "NOT_NULL_FLAG"
	case PriKeyFlag:
		return "PRI_KEY_FLAG"
	case UniqueKeyFlag:
		return "UNIQUE_KEY_FLAG"
	case MultipleKeyFlag:
		return "MULTIPLE_KEY_FLAG"
	case BlobFlag:
		return "BLOB_FLAG"
	case UnsignedFlag:
		return "UNSIGNED_FLAG"
	case ZerofillFlag:
		return "ZEROFILL_FLAG"
	case BinaryFlag:
		return "BINARY_FLAG"
	case EnumFlag:
		return "ENUM_FLAG"
	case AutoIncrementFlag:
		return "AUTO_INCREMENT_FLAG"
	case TimestampFlag:
		return "TIMESTAMP_FLAG"
	case SetFlag:
		return "SET_FLAG"
	default:
		return "Unknown ColumnDefinition"
	}
}

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
