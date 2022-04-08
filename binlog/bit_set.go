package binlog

import "fmt"

const (
	shift = 6
	mask  = 1<<6 - 1
)

type BitSet struct {
	words []uint64
}

func NewBitSet(bitLen int) (*BitSet, error) {
	if bitLen < 0 {
		return nil, fmt.Errorf("bitLen < 0")
	}

	b := new(BitSet)
	b.words = make([]uint64, (bitLen-1)>>shift+1)
	return b, nil
}

func (bs *BitSet) Get(index int) bool {
	wi := wordIndex(index)
	return (wi < len(bs.words)) &&
		(bs.words[wi]&(1<<(index&mask)) != 0)
}

func (bs *BitSet) Set(index int) {
	wi := wordIndex(index)
	if wi >= len(bs.words) {
		return
	}

	bs.words[wi] |= 1 << (index & mask)
}

func (bs *BitSet) SetValue(index int, val bool) {
	if val {
		bs.Set(index)
	} else {
		bs.Clear(index)
	}
}

func (bs *BitSet) Clear(index int) {
	wi := wordIndex(index)
	if wi >= len(bs.words) {
		return
	}

	bs.words[wi] &^= 1 << (index & mask)
}

func wordIndex(bitIndex int) int {
	return bitIndex >> shift
}
