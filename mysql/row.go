package mysql

import "strings"

type Row []ColumnValue

func (r Row) String() string {
	values := make([]string, len(r))
	for i, cv := range r {
		values[i] = cv.String()
	}
	return strings.Join(values, " | ")
}
