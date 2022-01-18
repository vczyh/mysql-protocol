package types

import (
	"testing"
)

func TestLengthEncodedString_Dump(t *testing.T) {
	dump := LengthEncodedString.Dump([]byte("_client_name"))
	t.Log(dump)
}
