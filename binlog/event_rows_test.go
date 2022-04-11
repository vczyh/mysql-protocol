package binlog

import "testing"

func TestTabletMapFlag(t *testing.T) {
	t.Log(TableMapFlagNoFlags)
	t.Log(TableMapFlagBitLenExact)
	t.Log(TableMapFlagReferredFKDB)
}
