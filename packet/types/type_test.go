package types

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestLengthEncodedString_Dump(t *testing.T) {
	dump := LengthEncodedString.Dump([]byte("_client_name"))
	t.Log(dump)
}

func TestName(t *testing.T) {
	fmt.Println(hex.Dump(FixedLengthInteger.Dump(uint64(6710628), 4)))

	val := FixedLengthInteger.Get([]byte{0x64, 0x65, 0x66, 0x00})
	t.Log(val)
}
