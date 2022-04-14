package mysql

import (
	"github.com/vczyh/mysql-protocol/packet"
	"time"
)

// Time stores information for TIMESTAMP, DATETIME, DATE, TIME.
// For TIMESTAMP andDATETIME, use unixMicro or (year, month, day, hour...).
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
	t packet.TableColumnType
}

func NewTimestamp(year, month, day, hour, minute, second, usec int, loc *time.Location) {
	newDateTime(year, month, day, hour, minute, second, usec, loc, packet.MySQLTypeTimestamp)
}

func NewDateTime(year, month, day, hour, minute, second, usec int, loc *time.Location) {
	newDateTime(year, month, day, hour, minute, second, usec, loc, packet.MySQLTypeDatetime)
}

func NewTimestampUnix(usec int64, loc *time.Location) Time {
	return Time{useUnix: true, unixMicro: usec, loc: loc, t: packet.MySQLTypeTimestamp}
}

func NewDateTimeUnix(usec int64, loc *time.Location) Time {
	return Time{useUnix: true, unixMicro: usec, loc: loc, t: packet.MySQLTypeDatetime}
}

func NewDate(year, month, day int) Time {
	return Time{
		year:  year,
		month: month,
		day:   day,
		t:     packet.MySQLTypeDate,
	}
}

func NewTime(negative bool, hour, minute, second, usec int) Time {
	return Time{
		hour:     hour,
		minute:   minute,
		second:   second,
		usec:     usec,
		negative: negative,
		t:        packet.MySQLTypeTime,
	}
}

func newDateTime(year, month, day, hour, minute, second, usec int, loc *time.Location, t packet.TableColumnType) Time {
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

func (t *Time) String() string {
	// TODO
	return ""
}
