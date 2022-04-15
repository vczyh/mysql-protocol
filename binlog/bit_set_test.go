package binlog

import (
	"github.com/vczyh/mysql-protocol/mysql"
	"testing"
)

func TestBitSet(t *testing.T) {
	set, err := mysql.NewBitSet(10)
	if err != nil {
		t.Fatal(err)
	}

	//t.Log(set.Get(0))
	//t.Log(set.Get(5))
	//t.Log(set.Get(10))

	set.SetValue(5, true)
	set.SetValue(5, true)
	set.SetValue(6, true)
	set.SetValue(6, true)
	set.SetValue(6, true)
	set.SetValue(0, true)

	t.Log(set.Get(5))
	//set.Clear(5)
	t.Log(set.Get(5))

	t.Log(set.Count())
}
