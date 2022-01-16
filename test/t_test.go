package test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestA(t *testing.T) {
	//s := "4a 00 00 00 0a 38 2e 30 2e 32 35 00 15 00 00 00 01 3e 6d 4c 14 49 75 4c 00 ff ff ff 02 00 ff cf 15 00 00 01 00 00 00 00 00 00 00 27 6b 43 5c 08 54 7c 36 25 6c 61 1d 00 6d 80 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72 64 00"
	//s:="54 00 00 01 8d a6 0f 00 00 00 00 01 08 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 70 61 6d 00 14 ab 09 ee f6 bc b1 32 3e 61 14 38 65 c0 99 1d 95 7d 75 d4 47 74 65 73 74 00 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72 64 00"
	s := "9a 00 00 01 05 a6 3a 00 ff ff ff 00 21 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 72 6f 6f 74 00 14 ab 3a 14 7d a0 78 f8 9b 4b 81 7e 7a 64 f5 e0 2b 73 65 a7 ae 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72 64 00 49 0c 5f 63 6c 69 65 6e 74 5f 6e 61 6d 65 07 70 79 6d 79 73 71 6c 04 5f 70 69 64 05 34 31 36 37 34 0f 5f 63 6c 69 65 6e 74 5f 76 65 72 73 69 6f 6e 05 31 2e 30 2e 32 0c 70 72 6f 67 72 61 6d 5f 6e 61 6d 65 05 6d 79 63 6c 69"
	//s := "9a 00 00 01 05 a6 3a 00 ff ff ff 00 21 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 72 6f 6f 74 00 14 ab 3a 14 7d a0 78 f8 9b 4b 81 7e 7a 64 f5 e0 2b 73 65 a7 ae 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73 77 6f 72 64 00 49 0c 5f 63 6c 69 65 6e 74 5f 6e 61 6d 65 07 70 79 6d 79 73 71 6c 04 5f 70 69 64 05 34 31 36 37 34 0f 5f 63 6c 69 65 6e 74 5f 76 65 72 73 69 6f 6e 05 31 2e 30 2e 32 0c 70 72 6f 67 72 61 6d 5f 6e 61 6d 65 05 6d 79 63 6c 69"
	arr := strings.Split(s, " ")
	var arr2 []string
	for _, a := range arr {
		arr2 = append(arr2, "0x"+a)
	}
	t.Log(strings.Join(arr2, ","))
}

func TestB(t *testing.T) {
	var v uint16
	if err := binary.Read(bytes.NewBuffer([]byte{0xff, 0xff}), binary.LittleEndian, &v); err != nil {
		t.Fatal(err)
	}
	t.Log(v)
}

func TestC(t *testing.T) {
	var bs = make([]byte, 4)
	binary.LittleEndian.PutUint16(bs, 65535)
	binary.LittleEndian.PutUint16(bs[2:], 53247)
	t.Log(bs)
	u := binary.LittleEndian.Uint32(bs)
	t.Logf("%x", u)
}

type User struct {
	Name string
}

func TestD(t *testing.T) {
	u := User{Name: "a"}
	m := map[int]User{
		1: u,
	}
	u.Name="b"
	t.Log(m)
}
