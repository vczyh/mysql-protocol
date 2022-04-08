package binlog

import "testing"

func TestBitSet(t *testing.T) {
	set, err := NewBitSet(10)
	if err != nil {
		t.Fatal(err)
	}

	//t.Log(set.Get(0))
	//t.Log(set.Get(5))
	//t.Log(set.Get(10))

	set.SetValue(5, true)
	t.Log(set.Get(5))
	set.Clear(5)
	t.Log(set.Get(5))

}
