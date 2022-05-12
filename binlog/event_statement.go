package binlog

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"strings"
	"time"
)

var (
	ErrUnsupportedQueryStatusVar = errors.New("unsupported query status var")
)

type QueryEvent struct {
	EventHeader
	ThreadId uint32
	ExecTime uint32
	//DatabaseLen   uint8
	ErrCode uint16
	//StatusVarsLen uint16
	//StatusVars []byte

	Flags2                 flag.Option
	SQLMode                flag.SQLMode
	Catalog                string
	AutoIncrementIncrement uint16
	AutoIncrementOffset    uint16
	//Charset                []byte // TODO type
	CharsetClient       *charset.Charset
	CollationConnection *charset.Collation
	CollationServer     *charset.Collation

	TimeZone string
	// TODO chang to name
	LcTimeNamesNum             uint16
	CollationDatabase          *charset.Collation
	TableMapForUpdate          uint64
	MasterDataWritten          uint32
	User                       string
	Host                       string
	MtsAccessedDBNames         []string
	ExplicitDefaultsTS         flag.Ternary
	DDLXid                     uint64
	DefaultCollationForUtf8mb4 *charset.Collation
	SQLRequirePrimaryKey       uint8
	DefaultTableEncryption     uint8

	Database string
	Query    string
}

func ParseQueryEvent(data []byte, fde *FormatDescriptionEvent) (e *QueryEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(QueryEvent)

	// TODO sure?
	// Default
	e.AutoIncrementIncrement = 1
	e.AutoIncrementOffset = 1

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Thread id
	if e.ThreadId, err = buf.Uint32(); err != nil {
		return nil, err
	}

	// Execute time
	if e.ExecTime, err = buf.Uint32(); err != nil {
		return nil, err
	}

	// Database length
	dbLen, err := buf.Uint8()
	if err != nil {
		return nil, err
	}

	// Error code
	if e.ErrCode, err = buf.Uint16(); err != nil {
		return nil, err
	}

	// TODO use post header len assert?
	if fde.BinlogVersion < 4 {
		return nil, ErrInvalidData
	}

	// Status vars length
	statusVarsLen, err := buf.Uint16()
	if err != nil {
		return nil, err
	}

	// Status vars
	l := buf.Len()
	for l-buf.Len() < int(statusVarsLen) {
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}

		switch flag.QueryEventStatusVars(b) {
		case flag.QueryStatusVarsFlags2:
			u, err := buf.Uint32()
			if err != nil {
				return nil, err
			}
			e.Flags2 = flag.Option(u)

		case flag.QueryStatusVarsSQLMode:
			u, err := buf.Uint64()
			if err != nil {
				return nil, err
			}
			e.SQLMode = flag.SQLMode(u)

		case flag.QueryStatusVarsCatalog:
			catalogLen, err := buf.Uint8()
			if err != nil {
				return nil, err
			}

			if catalogLen > 0 {
				next, err := buf.Next(int(catalogLen))
				if err != nil {
					return nil, err
				}
				e.Catalog = string(next)
				_, _ = buf.Next(1)
			}

		case flag.QueryStatusVarsAutoIncrement:
			if e.AutoIncrementIncrement, err = buf.Uint16(); err != nil {
				return nil, err
			}
			if e.AutoIncrementOffset, err = buf.Uint16(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsCharset:
			collationClientId, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			collationClient, err := charset.GetCollation(uint64(collationClientId))
			if err != nil {
				return nil, err
			}
			e.CharsetClient = collationClient.Charset()

			collationConnectionId, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			e.CollationConnection, err = charset.GetCollation(uint64(collationConnectionId))
			if err != nil {
				return nil, err
			}

			collationServerId, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			e.CollationServer, err = charset.GetCollation(uint64(collationServerId))
			if err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsTimeZone:
			timeZoneLen, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			if timeZoneLen > 0 {
				next, err := buf.Next(int(timeZoneLen))
				if err != nil {
					return nil, err
				}
				e.TimeZone = string(next)
			}

		case flag.QueryStatusVarsCatalogNz:
			catalogLen, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			if catalogLen > 0 {
				next, err := buf.Next(int(catalogLen))
				if err != nil {
					return nil, err
				}
				e.Catalog = string(next)
			}

		case flag.QueryStatusVarsLcTimeNames:
			if e.LcTimeNamesNum, err = buf.Uint16(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsCharsetDatabase:
			collationDatabaseId, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			if e.CollationDatabase, err = charset.GetCollation(uint64(collationDatabaseId)); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsTableMapForUpdate:
			if e.TableMapForUpdate, err = buf.Uint64(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsMasterDataWritten:
			if e.MasterDataWritten, err = buf.Uint32(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsInvoker:
			userLen, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			if userLen > 0 {
				if e.User, err = buf.NextString(int(userLen)); err != nil {
					return nil, err
				}
			}

			hostLen, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			if hostLen > 0 {
				if e.Host, err = buf.NextString(int(hostLen)); err != nil {
					return nil, err
				}
			}

		case flag.QueryStatusVarsUpdatedDBNames:
			mtsAccessedDBs, err := buf.Uint8()
			if err != nil {
				return nil, err
			}

			e.MtsAccessedDBNames = make([]string, mtsAccessedDBs)
			for i := uint8(0); i < mtsAccessedDBs; i++ {
				if e.MtsAccessedDBNames[i], err = buf.NulTerminatedString(); err != nil {
					return nil, err
				}
			}

		case flag.QueryStatusVarsMicroseconds:
			if e.EventHeader.Timestamp, err = buf.Uint24(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsExplicitDefaultsForTimestamp:
			val, err := buf.Uint8()
			if err != nil {
				return nil, err
			}

			e.ExplicitDefaultsTS = flag.TernaryOff
			if val != 0 {
				e.ExplicitDefaultsTS = flag.TernaryOn
			}

		case flag.QueryStatusVarsDDLLoggedWithXid:
			if e.DDLXid, err = buf.Uint64(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsDefaultCollationForUtf8mb4:
			defaultCollationForUtf8mb4Id, err := buf.Uint16()
			if err != nil {
				return nil, err
			}

			if e.DefaultCollationForUtf8mb4, err = charset.GetCollation(uint64(defaultCollationForUtf8mb4Id)); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsSQLRequirePrimaryKey:
			if e.SQLRequirePrimaryKey, err = buf.Uint8(); err != nil {
				return nil, err
			}

		case flag.QueryStatusVarsDefaultTableEncryption:
			if e.DefaultTableEncryption, err = buf.Uint8(); err != nil {
				return nil, err
			}

		default:
			return nil, ErrUnsupportedQueryStatusVar
		}
	}

	// Database
	if e.Database, err = buf.NextString(int(dbLen)); err != nil {
		return nil, err
	}

	// 0x00
	_, _ = buf.Next(1)

	// Query
	e.Query = buf.String()

	return e, nil
}

func (e *QueryEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Thread id: %d\n", e.ThreadId)
	fmt.Fprintf(sb, "Execute time: %s\n", time.Unix(int64(e.ExecTime), 0).Format(time.RFC3339))
	fmt.Fprintf(sb, "Error code: %d\n", e.ErrCode)

	queryOptions := make([]string, 4)
	queryOptions[0] = fmt.Sprintf("foreign_key_checks=%d", boolToInt(e.Flags2&flag.OptionNoForeignKeyChecks == 0))
	queryOptions[1] = fmt.Sprintf("sql_auto_is_null=%d", boolToInt(e.Flags2&flag.OptionAutoIsNull > 0))
	queryOptions[2] = fmt.Sprintf("unique_checks=%d", boolToInt(e.Flags2&flag.OptionRelaxedUniqueChecks == 0))
	queryOptions[3] = fmt.Sprintf("autocommit=%d", boolToInt(e.Flags2&flag.OptionNotAutocommit == 0))
	fmt.Fprintf(sb, "SET %s\n", strings.Join(queryOptions, ", "))

	fmt.Fprintf(sb, "SET sql_mode=%d\n", e.SQLMode)
	fmt.Fprintf(sb, "SET auto_increment_increment=%d, auto_increment_offset=%d\n", e.AutoIncrementIncrement, e.AutoIncrementOffset)

	charset := make([]string, 3)
	if e.CharsetClient != nil {
		charset[0] = fmt.Sprintf("character_set_client=%s", e.CharsetClient.Name())
	}
	if e.CollationConnection != nil {
		charset[1] = fmt.Sprintf("collation_connection=%s(%d)", e.CollationConnection.Name(), e.CollationConnection.Id())
	}
	if e.CollationServer != nil {
		charset[2] = fmt.Sprintf("collation_server=%s(%d)", e.CollationServer.Name(), e.CollationServer.Id())
	}
	fmt.Fprintf(sb, "SET %s\n", strings.Join(charset, ", "))

	if e.TimeZone != "" {
		fmt.Fprintf(sb, "%s\n", e.TimeZone)
	}

	fmt.Fprintf(sb, "SET lc_time_names=%d\n", e.LcTimeNamesNum)

	if e.CollationDatabase == nil {
		fmt.Fprintf(sb, "SET collation_database=DEFAULT\n")
	} else {
		fmt.Fprintf(sb, "SET collation_database=%s\n", e.CollationDatabase.Name())
	}

	if e.DefaultCollationForUtf8mb4 != nil {
		fmt.Fprintf(sb, "SET default_collation_for_utf8mb4=%s(%d)\n", e.DefaultCollationForUtf8mb4.Name(), e.DefaultCollationForUtf8mb4.Id())
	}

	fmt.Fprintf(sb, "Database: %s\n", e.Database)
	fmt.Fprintf(sb, "Query: %s\n", e.Query)

	return sb.String()
}

type RandEvent struct {
	EventHeader
	Seed1 uint64
	Seed2 uint64
}

func ParseRandEvent(data []byte) (e *RandEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(RandEvent)

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse seed1 and seed2.
	if e.Seed1, err = buf.Uint64(); err != nil {
		return nil, err
	}
	if e.Seed2, err = buf.Uint64(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *RandEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Seed1: %d\n", e.Seed1)
	fmt.Fprintf(sb, "Seed2: %d\n", e.Seed1)

	return sb.String()
}

type UserVarEvent struct {
	EventHeader
	Name    string
	IsNull  bool
	Type    UserVarValueType
	Charset *charset.Charset

	// UserVarValueTypeString: string
	// UserVarValueTypeReal: float64
	// UserVarValueTypeInt: int64 or uint64(Flags&UserVarFlagUnsigned!=0)
	// UserVarValueTypeDecimal: decimal(string)
	Value interface{}

	Flags UserVarFlag
}

type UserVarValueType uint8

const (
	// UserVarValueTypeString represents char.
	UserVarValueTypeString UserVarValueType = 0

	// UserVarValueTypeReal represents double.
	UserVarValueTypeReal UserVarValueType = 1

	// UserVarValueTypeInt represents long long.
	UserVarValueTypeInt UserVarValueType = 2

	// UserVarValueTypeDecimal represents char, to be converted to/from a decimal.
	UserVarValueTypeDecimal UserVarValueType = 4
)

func (t UserVarValueType) String() string {
	switch t {
	case UserVarValueTypeString:
		return "String"
	case UserVarValueTypeReal:
		return "Double"
	case UserVarValueTypeInt:
		return "Int"
	case UserVarValueTypeDecimal:
		return "Decimal"
	default:
		return "Unknown UserVarValueType"
	}
}

type UserVarFlag uint8

const (
	UserVarFlagUndef UserVarFlag = iota
	UserVarFlagUnsigned
)

func ParseUserVarEvent(data []byte) (e *UserVarEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(UserVarEvent)

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse variable name.
	nameLen, err := buf.Uint32()
	if err != nil {
		return nil, err
	}
	if e.Name, err = buf.NextString(int(nameLen)); err != nil {
		return nil, err
	}

	// Parse whether value is null.
	u, err := buf.Uint8()
	if err != nil {
		return nil, err
	}
	if u == 1 {
		e.IsNull = true
	}

	if e.IsNull {
		e.Type = UserVarValueTypeString
		collation, err := charset.GetCollation(63)
		if err != nil {
			return nil, err
		}
		e.Charset = collation.Charset()
	} else {
		// Parse variable type.
		u, err = buf.Uint8()
		if err != nil {
			return nil, err
		}
		e.Type = UserVarValueType(u)

		// Parse charset
		u, err := buf.Uint32()
		if err != nil {
			return nil, err
		}
		collation, err := charset.GetCollation(uint64(u))
		if err != nil {
			return nil, err
		}
		e.Charset = collation.Charset()

		// Parse value.
		valueLen, err := buf.Uint32()
		if err != nil {
			return nil, err
		}

		switch e.Type {
		case UserVarValueTypeString:
			// TODO other charset?
			if e.Value, err = buf.NextString(int(valueLen)); err != nil {
				return nil, err
			}

		case UserVarValueTypeReal:
			if e.Value, err = buf.Float64(); err != nil {
				return nil, err
			}

		case UserVarValueTypeInt:
			if valueLen != 8 {
				return nil, fmt.Errorf("invalid value length %d for UserVarValueTypeInt", valueLen)
			}
			val, err := buf.Uint64()
			if err != nil {
				return nil, err
			}
			e.Value = int64(val)

		case UserVarValueTypeDecimal:
			precision, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			decimals, err := buf.Uint8()
			if err != nil {
				return nil, err
			}
			if e.Value, err = buf.Decimal(int(precision), int(decimals)); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("invalid UserVarValueType %d", e.Type)
		}
	}

	// Parse flags.
	e.Flags = UserVarFlagUndef
	if buf.Len() > 0 {
		u, err = buf.Uint8()
		if err != nil {
			return nil, err
		}
		e.Flags = UserVarFlag(u)

		if e.Flags&UserVarFlagUnsigned != 0 {
			e.Value = uint64(e.Value.(int64))
		}
	}

	return e, nil
}

func (e *UserVarEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Type: %s\n", e.Type)
	fmt.Fprintf(sb, "Charset: %s\n", e.Charset.Name())
	fmt.Fprintf(sb, "Flags: %d\n", e.Flags)

	switch {
	case e.IsNull:
		fmt.Fprintf(sb, "SET @%s := NULL\n", e.Name)
	case e.Type == UserVarValueTypeString:
		fmt.Fprintf(sb, "SET @%s := '%s'\n", e.Name, e.Value)
	default:
		fmt.Fprintf(sb, "SET @%s:=%v\n", e.Name, e.Value)
	}

	return sb.String()
}

type IntVarEvent struct {
	EventHeader
	Type  IntVarType
	Value uint64
}

type IntVarType uint8

const (
	IntVarTypeInvalidInt IntVarType = iota

	// IntVarTypeLastInsertId indicates the value to use for the LAST_INSERT_ID() function in the next statement.
	IntVarTypeLastInsertId

	// IntVarTypeInsertId indicates the value to use for an AUTO_INCREMENT column in the next statement.
	IntVarTypeInsertId
)

func (t IntVarType) String() string {
	switch t {
	case IntVarTypeInvalidInt:
		return "InvalidInt"
	case IntVarTypeLastInsertId:
		return "LastInsertId"
	case IntVarTypeInsertId:
		return "InsertId"
	default:
		return "UnknownIntVarType"
	}
}

func ParseIntVarEvent(data []byte) (e *IntVarEvent, err error) {
	buf := mysql.NewBuffer(data)
	e = new(IntVarEvent)

	// Parse event header.
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Parse type.
	u, err := buf.Uint8()
	if err != nil {
		return nil, err
	}
	e.Type = IntVarType(u)

	// Parse value.
	if e.Value, err = buf.Uint64(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *IntVarEvent) String() string {
	sb := new(strings.Builder)
	sb.WriteString(e.EventHeader.String())

	fmt.Fprintf(sb, "Type: %s\n", e.Type)
	fmt.Fprintf(sb, "Value: %d\n", e.Value)

	return sb.String()
}
