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
	fmt.Println((bitLen-1)>>shift + 1)
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

func (bs *BitSet) Count() (count int) {
	for _, word := range bs.words {
		count += swar(word)
	}
	return count
}

func wordIndex(bitIndex int) int {
	return bitIndex >> shift
}

func swar(i uint64) int {
	i = (i & 0x5555555555555555) + ((i >> 1) & 0x5555555555555555)
	i = (i & 0x3333333333333333) + ((i >> 2) & 0x3333333333333333)
	i = (i & 0x0F0F0F0F0F0F0F0F) + ((i >> 4) & 0x0F0F0F0F0F0F0F0F)
	i = (i * 0x0101010101010101) >> 56
	return int(i)
}
