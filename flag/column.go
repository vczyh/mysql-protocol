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
