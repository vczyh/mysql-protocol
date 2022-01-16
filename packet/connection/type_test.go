package connection

import (
	"mysql-protocol/packet/types"
	"testing"
)

func TestLengthEncodedString_Dump(t *testing.T) {
	dump := types.LengthEncodedString.Dump([]byte("_client_name"))
	t.Log(dump)
}
