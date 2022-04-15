package binlog

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
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

	Flags2                 Option
	SQLMode                SQLMode
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
	ExplicitDefaultsTS         Ternary
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

		switch QueryEventStatusVars(b) {
		case QueryStatusVarsFlags2:
			u, err := buf.Uint32()
			if err != nil {
				return nil, err
			}
			e.Flags2 = Option(u)

		case QueryStatusVarsSQLMode:
			u, err := buf.Uint64()
			if err != nil {
				return nil, err
			}
			e.SQLMode = SQLMode(u)

		case QueryStatusVarsCatalog:
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

		case QueryStatusVarsAutoIncrement:
			if e.AutoIncrementIncrement, err = buf.Uint16(); err != nil {
				return nil, err
			}
			if e.AutoIncrementOffset, err = buf.Uint16(); err != nil {
				return nil, err
			}

		case QueryStatusVarsCharset:
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

		case QueryStatusVarsTimeZone:
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

		case QueryStatusVarsCatalogNz:
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

		case QueryStatusVarsLcTimeNames:
			if e.LcTimeNamesNum, err = buf.Uint16(); err != nil {
				return nil, err
			}

		case QueryStatusVarsCharsetDatabase:
			collationDatabaseId, err := buf.Uint16()
			if err != nil {
				return nil, err
			}
			if e.CollationDatabase, err = charset.GetCollation(uint64(collationDatabaseId)); err != nil {
				return nil, err
			}

		case QueryStatusVarsTableMapForUpdate:
			if e.TableMapForUpdate, err = buf.Uint64(); err != nil {
				return nil, err
			}

		case QueryStatusVarsMasterDataWritten:
			if e.MasterDataWritten, err = buf.Uint32(); err != nil {
				return nil, err
			}

		case QueryStatusVarsInvoker:
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

		case QueryStatusVarsUpdatedDBNames:
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

		case QueryStatusVarsMicroseconds:
			if e.EventHeader.Timestamp, err = buf.Uint24(); err != nil {
				return nil, err
			}

		case QueryStatusVarsExplicitDefaultsForTimestamp:
			val, err := buf.Uint8()
			if err != nil {
				return nil, err
			}

			e.ExplicitDefaultsTS = TernaryOff
			if val != 0 {
				e.ExplicitDefaultsTS = TernaryOn
			}

		case QueryStatusVarsDDLLoggedWithXid:
			if e.DDLXid, err = buf.Uint64(); err != nil {
				return nil, err
			}

		case QueryStatusVarsDefaultCollationForUtf8mb4:
			defaultCollationForUtf8mb4Id, err := buf.Uint16()
			if err != nil {
				return nil, err
			}

			if e.DefaultCollationForUtf8mb4, err = charset.GetCollation(uint64(defaultCollationForUtf8mb4Id)); err != nil {
				return nil, err
			}

		case QueryStatusVarsSQLRequirePrimaryKey:
			if e.SQLRequirePrimaryKey, err = buf.Uint8(); err != nil {
				return nil, err
			}

		case QueryStatusVarsDefaultTableEncryption:
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
	queryOptions[0] = fmt.Sprintf("foreign_key_checks=%d", boolToInt(e.Flags2&OptionNoForeignKeyChecks == 0))
	queryOptions[1] = fmt.Sprintf("sql_auto_is_null=%d", boolToInt(e.Flags2&OptionAutoIsNull > 0))
	queryOptions[2] = fmt.Sprintf("unique_checks=%d", boolToInt(e.Flags2&OptionRelaxedUniqueChecks == 0))
	queryOptions[3] = fmt.Sprintf("autocommit=%d", boolToInt(e.Flags2&OptionNotAutocommit == 0))
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
