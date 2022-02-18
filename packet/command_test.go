package packet

import (
	"strings"
	"testing"
	"time"
)

func TestParseColumnCount(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x01, 0x01}
	columnCount, err := ParseQueryResponse(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(columnCount)
}

func TestParseColumnDefinition(t *testing.T) {
	data := []byte{
		0x17, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00, 0x06, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x00,
	}
	cd, err := ParseColumnDefinition(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(cd.Name)
	t.Log(cd.CharacterSet)
	t.Log(cd.ColumnType)
}

func TestParseTextResultSetRow(t *testing.T) {
	data := []byte{
		0x17, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00, 0x06, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x00,
	}
	cd, err := ParseColumnDefinition(data)
	if err != nil {
		t.Fatal(err)
	}

	data = []byte{0x06, 0x00, 0x00, 0x04,
		0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	}
	row, err := ParseTextResultSetRow(data, []*ColumnDefinition{cd}, time.Local)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(row.Values)
}

func TestBuildTestData(t *testing.T) {
	s := "17 00 00 02 00 00 00 00 06 63 6f 6c 75 6d 6e 00 00 ff 00 00 00 00 00 fd 00 00 00"
	arr := strings.Split(s, " ")
	for i := range arr {
		arr[i] = "0x" + arr[i]
	}
	str := strings.Join(arr, ",")
	t.Log(str)
}
