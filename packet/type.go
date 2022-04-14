package packet

import (
	"encoding/binary"
	"io"
)

var FixedLengthInteger fixedLengthInteger

// https://dev.mysql.com/doc/internals/en/integer.html#fixed-length-integer
type fixedLengthInteger struct{}

func (fixedLengthInteger) Get(bs []byte) uint64 {
	return binary.LittleEndian.Uint64(byteAlignment(bs, 8))
}

func (fixedLengthInteger) Uint64(bs []byte) uint64 {
	return binary.LittleEndian.Uint64(byteAlignment(bs, 8))
}

func (fixedLengthInteger) Uint32(bs []byte) uint32 {
	return binary.LittleEndian.Uint32(byteAlignment(bs, 4))
}

func (fixedLengthInteger) Uint16(bs []byte) uint16 {
	return binary.LittleEndian.Uint16(byteAlignment(bs, 2))
}

func (fixedLengthInteger) Dump(v uint64, len int) []byte {
	switch len {
	case 1:
		return []byte{byte(v)}
	case 2:
		return []byte{byte(v), byte(v >> 8)}
	case 3:
		return []byte{byte(v), byte(v >> 8), byte(v >> 16)}
	case 4:
		return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
	case 6:
		return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24), byte(v >> 32), byte(v >> 40)}
	case 8:
		return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24), byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
	default:
		return []byte{}
	}
}

var LengthEncodedInteger lengthEncodedInteger

// https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer
type lengthEncodedInteger struct{}

func (lengthEncodedInteger) Get(r io.Reader) (uint64, error) {
	bs := make([]byte, 1)
	_, err := r.Read(bs)
	if err != nil {
		return 0, err
	}
	// integer data size
	var size int
	val := bs[0]
	if val < 0xfb {
		return uint64(val), nil
	} else if val == 0xfc {
		size = 2
	} else if val == 0xfd {
		size = 3
	} else if val == 0xfe {
		size = 8
	}
	bs = make([]byte, size)
	_, err = r.Read(bs)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(byteAlignment(bs, 8)), nil
}

func (lengthEncodedInteger) Dump(v uint64) []byte {
	switch {
	case v < 251:
		return []byte{byte(v)}
	case v < 0xffff:
		bs := make([]byte, 3)
		bs[0] = 0xfc
		binary.LittleEndian.PutUint16(bs[1:], uint16(v))
		return bs
	case v < 0xffffff:
		bs := make([]byte, 5)
		bs[0] = 0xfd
		binary.LittleEndian.PutUint32(bs[1:], uint32(v))
		return bs[:len(bs)-1]
	case v < 0xffffffffffffffff:
		bs := make([]byte, 9)
		bs[0] = 0xfe
		binary.LittleEndian.PutUint64(bs[1:], v)
		return bs
	default:
		return []byte{}
	}
}

func byteAlignment(bs []byte, destLen int) []byte {
	dest := append([]byte{}, bs...)
	l := len(dest)
	for l < destLen {
		dest = append(dest, 0x00)
		l++
	}
	return dest
}

var NulTerminatedString nulTerminatedString

type nulTerminatedString struct{}

func (nulTerminatedString) Get(r io.Reader) ([]byte, error) {
	var data []byte
	var bs = make([]byte, 1)
	for {
		_, err := r.Read(bs)
		if err != nil {
			return nil, err
		}
		if bs[0] == 0x00 {
			break
		}
		data = append(data, bs[0])
	}
	return data, nil
}

func (nulTerminatedString) Dump(bs []byte) []byte {
	dump := make([]byte, len(bs)+1)
	copy(dump, bs)
	return dump
}

var LengthEncodedString lengthEncodedString

type lengthEncodedString struct{}

func (lengthEncodedString) Get(r io.Reader) ([]byte, error) {
	l, err := LengthEncodedInteger.Get(r)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, l)
	_, err = r.Read(bs)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func (lengthEncodedString) Dump(bs []byte) []byte {
	dump := LengthEncodedInteger.Dump(uint64(len(bs)))
	return append(dump, bs...)
}
