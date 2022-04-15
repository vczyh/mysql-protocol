package mysql

import (
	"bytes"
	"encoding/binary"
	"github.com/vczyh/mysql-protocol/packet"
	"io"
)

type Buffer struct {
	buf *bytes.Buffer
}

func NewBuffer(p []byte) *Buffer {
	return &Buffer{buf: bytes.NewBuffer(p)}
}

// Uint8 reads one byte from the buffer, and returns uint8.
func (b *Buffer) Uint8() (uint8, error) {
	return b.ReadByte()
}

// Int8 reads one byte from the buffer, and returns int8.
func (b *Buffer) Int8() (int8, error) {
	u, err := b.Uint8()
	return int8(u), err
}

// Uint16 reads 2 bytes from the buffer, and returns little endian uint16.
func (b *Buffer) Uint16() (uint16, error) {
	p, err := b.Next(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(p), nil
}

// BUint16 reads 2 bytes from the buffer, and returns big endian uint16.
func (b *Buffer) BUint16() (uint16, error) {
	p, err := b.Next(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(p), nil
}

// Int16 reads 2 bytes from the buffer, and returns little endian Int16.
func (b *Buffer) Int16() (int16, error) {
	u, err := b.Uint16()
	return int16(u), err
}

// Uint24 reads 3 bytes from the buffer, and returns little endian uint32.
func (b *Buffer) Uint24() (uint32, error) {
	p, err := b.Next(3)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 4)
	copy(dst, p)
	return binary.LittleEndian.Uint32(dst), nil
}

// BUint24 reads 3 bytes from the buffer, and returns big endian uint32.
func (b *Buffer) BUint24() (uint32, error) {
	p, err := b.Next(3)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 4)
	copy(dst, p)
	return binary.BigEndian.Uint32(dst), nil
}

// Int24 reads 3 bytes from the buffer, and returns little endian int32.
func (b *Buffer) Int24() (int32, error) {
	u, err := b.Uint24()
	return int32(u), err
}

// Uint32 reads 4 bytes from the buffer, and returns little endian uint32.
func (b *Buffer) Uint32() (uint32, error) {
	p, err := b.Next(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p), nil
}

// BUint32 reads 4 bytes from the buffer, and returns big endian uint32.
func (b *Buffer) BUint32() (uint32, error) {
	p, err := b.Next(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(p), nil
}

// Int32 reads 4 bytes from the buffer, and returns little endian int32.
func (b *Buffer) Int32() (int32, error) {
	u, err := b.Uint32()
	return int32(u), err
}

// Uint40 reads 5 bytes from the buffer, and returns little endian uint64.
func (b *Buffer) Uint40() (uint64, error) {
	p, err := b.Next(5)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 8)
	copy(dst, p)
	return binary.LittleEndian.Uint64(dst), nil
}

// BUint40 reads 5 bytes from the buffer, and returns big endian uint64.
func (b *Buffer) BUint40() (uint64, error) {
	p, err := b.Next(5)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 8)
	copy(dst, p)
	return binary.BigEndian.Uint64(dst), nil
}

// Int40 reads 5 bytes from the buffer, and returns little endian int64.
func (b *Buffer) Int40() (int64, error) {
	u, err := b.Uint40()
	return int64(u), err
}

// Uint48 reads 6 bytes from the buffer, and returns little endian uint64.
func (b *Buffer) Uint48() (uint64, error) {
	p, err := b.Next(6)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 8)
	copy(dst, p)
	return binary.LittleEndian.Uint64(dst), nil
}

// Uint56 reads 7 bytes from the buffer, and returns little endian uint64.
func (b *Buffer) Uint56() (uint64, error) {
	p, err := b.Next(7)
	if err != nil {
		return 0, err
	}
	dst := make([]byte, 8)
	copy(dst, p)
	return binary.LittleEndian.Uint64(dst), nil
}

// Uint64 reads 8 bytes from the buffer, and returns little endian uint64.
func (b *Buffer) Uint64() (uint64, error) {
	p, err := b.Next(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(p), nil
}

// Int64 reads 8 bytes from the buffer, and returns little endian int64.
func (b *Buffer) Int64() (int64, error) {
	u, err := b.Uint64()
	return int64(u), err
}

// LengthEncodedUint64 reads bytes according to the Protocol:LengthEncodedInteger,
// and returns little endian int64.
func (b *Buffer) LengthEncodedUint64() (uint64, error) {
	return packet.LengthEncodedInteger.Get(b)
}

// LengthEncodedUint32 reads bytes according to the Protocol:LengthEncodedInteger,
// and returns little endian int32.
func (b *Buffer) LengthEncodedUint32() (uint32, error) {
	u, err := packet.LengthEncodedInteger.Get(b)
	return uint32(u), err
}

// LengthEncodedInt reads bytes according to the Protocol:LengthEncodedInteger,
// and returns little endian int.
func (b *Buffer) LengthEncodedInt() (int, error) {
	u, err := packet.LengthEncodedInteger.Get(b)
	return int(u), err
}

// NulTerminatedBytes reads bytes according to the Protocol:NulTerminatedString.
func (b *Buffer) NulTerminatedBytes() ([]byte, error) {
	return packet.NulTerminatedString.Get(b)
}

// NulTerminatedString works the same way as NulTerminatedBytes, but it returns string.
func (b *Buffer) NulTerminatedString() (string, error) {
	data, err := packet.NulTerminatedString.Get(b)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// LengthEncodedBytes reads bytes according to the Protocol:LengthEncodedString.
func (b *Buffer) LengthEncodedBytes() ([]byte, error) {
	return packet.LengthEncodedString.Get(b)
}

// LengthEncodedString works the same way as LengthEncodedBytes, but it returns string.
func (b *Buffer) LengthEncodedString() (string, error) {
	data, err := b.LengthEncodedBytes()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// CreateBitmap reads (cnt+7)/8 bytes and returns BitSet.
func (b *Buffer) CreateBitmap(cnt int) (*BitSet, error) {
	bs, err := NewBitSet(cnt)
	if err != nil {
		return nil, err
	}

	for bit := 0; bit < cnt; bit += 8 {
		flag, err := b.ReadByte()
		if err != nil {
			return nil, err
		}

		if flag == 0 {
			continue
		}

		if (flag & 0x01) != 0 {
			bs.Set(bit)
		}
		if (flag & 0x02) != 0 {
			bs.Set(bit + 1)
		}
		if (flag & 0x04) != 0 {
			bs.Set(bit + 2)
		}
		if (flag & 0x08) != 0 {
			bs.Set(bit + 3)
		}
		if (flag & 0x10) != 0 {
			bs.Set(bit + 4)
		}
		if (flag & 0x20) != 0 {
			bs.Set(bit + 5)
		}
		if (flag & 0x40) != 0 {
			bs.Set(bit + 6)
		}
		if (flag & 0x80) != 0 {
			bs.Set(bit + 7)
		}
	}

	return bs, nil
}

// ReadByte reads and returns the next byte from the buffer.
// If no byte is available, it returns error io.EOF.
func (b *Buffer) ReadByte() (byte, error) {
	return b.buf.ReadByte()
}

// Read reads the next len(p) bytes from the buffer or until the buffer
// is drained. The return value n is the number of bytes read. If the
// buffer has no data to return, err is io.EOF (unless len(p) is zero);
// otherwise it is nil.
func (b *Buffer) Read(p []byte) (int, error) {
	return b.buf.Read(p)
}

// Next reads the next n bytes from the buffer.
// If there are less than n bytes (b.Len() < n), it returns error io.EOF.
func (b *Buffer) Next(n int) ([]byte, error) {
	p := make([]byte, n)
	n2, err := b.Read(p)
	if err != nil {
		return nil, err
	}
	if n != n2 {
		return nil, io.EOF
	}
	return p, nil
}

func (b *Buffer) NextString(n int) (string, error) {
	next, err := b.Next(n)
	if err != nil {
		return "", err
	}
	return string(next), nil
}

// Bytes returns a slice of length b.Len() holding the unread portion of the buffer.
// The slice is valid for use only until the next buffer modification (that is,
// only until the next call to a method like Read, Write, Reset, or Truncate).
// The slice aliases the buffer content at least until the next buffer modification,
// so immediate changes to the slice will affect the result of future reads.
func (b *Buffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *Buffer) String() string {
	return b.buf.String()
}

// Len returns the number of bytes of the unread portion of the buffer;
// b.Len() == len(b.Bytes()).
func (b *Buffer) Len() int {
	return b.buf.Len()
}
