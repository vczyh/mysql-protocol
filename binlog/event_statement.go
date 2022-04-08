package binlog

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/packet"
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

func ParseQueryEvent(data []byte, fde *FormatDescriptionEvent) (*QueryEvent, error) {
	buf := bytes.NewBuffer(data)
	e := new(QueryEvent)

	// Default
	e.AutoIncrementIncrement = 1
	e.AutoIncrementOffset = 1

	// Event header
	if err := FillEventHeader(&e.EventHeader, buf); err != nil {
		return nil, err
	}

	// Thread id
	e.ThreadId = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Execute time
	e.ExecTime = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))

	// Database length
	dbLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	// Error code
	e.ErrCode = uint16(packet.FixedLengthInteger.Get(buf.Next(2)))

	// TODO use post header len assert?
	if fde.BinlogVersion < 4 {
		return nil, ErrInvalidData
	}
	// Status vars length
	statusVarsLen := int(packet.FixedLengthInteger.Get(buf.Next(2)))

	// Status vars
	l := buf.Len()
	for l-buf.Len() < statusVarsLen {
		switch QueryEventStatusVars(buf.Next(1)[0]) {
		case QueryStatusVarsFlags2:
			e.Flags2 = Option(packet.FixedLengthInteger.Get(buf.Next(4)))
		case QueryStatusVarsSQLMode:
			e.SQLMode = SQLMode(packet.FixedLengthInteger.Get(buf.Next(8)))
		case QueryStatusVarsCatalog:
			catalogLen, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			if catalogLen > 0 {
				e.Catalog = string(buf.Next(int(catalogLen)))
				buf.Next(1)
			}
		case QueryStatusVarsAutoIncrement:
			e.AutoIncrementIncrement = uint16(packet.FixedLengthInteger.Get(buf.Next(2)))
			e.AutoIncrementOffset = uint16(packet.FixedLengthInteger.Get(buf.Next(2)))
		case QueryStatusVarsCharset:
			collationClientId := packet.FixedLengthInteger.Get(buf.Next(2))
			collationClient, err := charset.GetCollation(collationClientId)
			if err != nil {
				return nil, err
			}
			e.CharsetClient = collationClient.Charset()

			collationConnectionId := packet.FixedLengthInteger.Get(buf.Next(2))
			e.CollationConnection, err = charset.GetCollation(collationConnectionId)
			if err != nil {
				return nil, err
			}

			collationServerId := packet.FixedLengthInteger.Get(buf.Next(2))
			e.CollationServer, err = charset.GetCollation(collationServerId)
			if err != nil {
				return nil, err
			}
		case QueryStatusVarsTimeZone:
			timeZoneLen, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			if timeZoneLen > 0 {
				e.TimeZone = string(buf.Next(int(timeZoneLen)))
			}
		case QueryStatusVarsCatalogNz:
			catalogLen, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			if catalogLen > 0 {
				e.Catalog = string(buf.Next(int(catalogLen)))
			}
		case QueryStatusVarsLcTimeNames:
			e.LcTimeNamesNum = uint16(packet.FixedLengthInteger.Get(buf.Next(2)))
		case QueryStatusVarsCharsetDatabase:
			collationDatabaseId := packet.FixedLengthInteger.Get(buf.Next(2))
			e.CollationDatabase, err = charset.GetCollation(collationDatabaseId)
			if err != nil {
				return nil, err
			}
		case QueryStatusVarsTableMapForUpdate:
			e.TableMapForUpdate = packet.FixedLengthInteger.Get(buf.Next(8))
		case QueryStatusVarsMasterDataWritten:
			e.MasterDataWritten = uint32(packet.FixedLengthInteger.Get(buf.Next(4)))
		case QueryStatusVarsInvoker:
			userLen, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			if userLen > 0 {
				e.User = string(buf.Next(int(userLen)))
			}
			hostLen, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			if hostLen > 0 {
				e.Host = string(buf.Next(int(hostLen)))
			}
		case QueryStatusVarsUpdatedDBNames:
			mtsAccessedDBs, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			e.MtsAccessedDBNames = make([]string, int(mtsAccessedDBs))
			for i := uint8(0); i < mtsAccessedDBs; i++ {
				b, err := packet.NulTerminatedString.Get(buf)
				if err != nil {
					return nil, err
				}
				e.MtsAccessedDBNames[i] = string(b)
			}
		case QueryStatusVarsMicroseconds:
			e.EventHeader.Timestamp = uint32(packet.FixedLengthInteger.Get(buf.Next(3)))
		case QueryStatusVarsExplicitDefaultsForTimestamp:
			val, err := buf.ReadByte()
			if err != nil {
				return nil, err
			}
			e.ExplicitDefaultsTS = TernaryOff
			if val != 0 {
				e.ExplicitDefaultsTS = TernaryOn
			}
		case QueryStatusVarsDDLLoggedWithXid:
			e.DDLXid = packet.FixedLengthInteger.Get(buf.Next(8))
		case QueryStatusVarsDefaultCollationForUtf8mb4:
			defaultCollationForUtf8mb4Id := packet.FixedLengthInteger.Get(buf.Next(2))
			e.DefaultCollationForUtf8mb4, err = charset.GetCollation(defaultCollationForUtf8mb4Id)
			if err != nil {
				return nil, err
			}
		case QueryStatusVarsSQLRequirePrimaryKey:
			e.SQLRequirePrimaryKey, err = buf.ReadByte()
			if err != nil {
				return nil, err
			}
		case QueryStatusVarsDefaultTableEncryption:
			e.DefaultTableEncryption, err = buf.ReadByte()
			if err != nil {
				return nil, err
			}
		default:
			return nil, ErrUnsupportedQueryStatusVar
		}
	}

	// Database
	e.Database = string(buf.Next(int(dbLen)))

	// 0x00
	buf.Next(1)

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
