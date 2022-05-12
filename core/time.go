package core

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/flag"
	"time"
)

// Time stores information for TIMESTAMP, DATETIME, DATE, TIME.
// For TIMESTAMP and DATETIME, use unixMicro or (year, month, day, hour...).
type Time struct {
	useUnix bool

	unixMicro int64

	year, month, day     int
	hour, minute, second int
	usec                 int

	// It is used by TIME.
	negative bool

	// It is used by TIMESTAMP, DATETIME.
	// The nil location means time.UTC.
	loc *time.Location

	// It represents what type of this.
	t flag.TableColumnType
}

func NewTimestamp(year, month, day, hour, minute, second, usec int, loc *time.Location) Time {
	return newDateTime(year, month, day, hour, minute, second, usec, loc, flag.MySQLTypeTimestamp)
}

func NewDateTime(year, month, day, hour, minute, second, usec int, loc *time.Location) Time {
	return newDateTime(year, month, day, hour, minute, second, usec, loc, flag.MySQLTypeDatetime)
}

func NewTimestampUnix(usec int64, loc *time.Location) Time {
	return Time{useUnix: true, unixMicro: usec, loc: loc, t: flag.MySQLTypeTimestamp}
}

func NewDateTimeUnix(usec int64, loc *time.Location) Time {
	return Time{useUnix: true, unixMicro: usec, loc: loc, t: flag.MySQLTypeDatetime}
}

func NewDate(year, month, day int) Time {
	return Time{
		year:  year,
		month: month,
		day:   day,
		t:     flag.MySQLTypeDate,
	}
}

func NewTime(negative bool, hour, minute, second, usec int) Time {
	return Time{
		hour:     hour,
		minute:   minute,
		second:   second,
		usec:     usec,
		negative: negative,
		t:        flag.MySQLTypeTime,
	}
}

func (t *Time) String() string {
	// TODO
	return ""
}

func newDateTime(year, month, day, hour, minute, second, usec int, loc *time.Location, t flag.TableColumnType) Time {
	return Time{
		year:   year,
		month:  month,
		day:    day,
		hour:   hour,
		minute: minute,
		second: second,
		usec:   usec,
		loc:    loc,
		t:      t,
	}
}

func TimeFromInt64(fieldType flag.TableColumnType, value int64, loc *time.Location) (Time, error) {
	switch fieldType {
	case flag.MySQLTypeTime:
		return TimeFromInt64Time(value), nil
	case flag.MySQLTypeDate:
		return TimeFromInt64Time(value), nil
	case flag.MySQLTypeDatetime, flag.MySQLTypeTimestamp:
		return TimeFromInt64DateTime(value, loc), nil
	default:
		return Time{}, fmt.Errorf("unsupported time type %s", fieldType)
	}
}

func TimeFromInt64Time(tmp int64) Time {
	neg := tmp < 0
	if neg {
		tmp = -tmp
	}

	hms := tmp >> 24
	hour := (hms >> 12) % (1 << 10)
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	usec := tmp % (1 << 24)

	return NewTime(neg, int(hour), int(minute), int(second), int(usec))
}

func TimeFromInt64Date(tmp int64) Time {
	t := TimeFromInt64DateTime(tmp, nil)
	return NewDate(t.year, t.month, t.day)
}

func TimeFromInt64DateTime(tmp int64, loc *time.Location) Time {
	ymdhms := tmp >> 24
	frac := tmp % (1 << 24)

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := ymd % (1 << 5)
	month := ym % 13
	year := ym / 13

	second := hms % (1 << 6)
	minute := (hms >> 6) % (1 << 6)
	hour := hms >> 12

	return NewDateTime(int(year), int(month), int(day), int(hour), int(minute), int(second), int(frac), loc)
}
