package generic

import "mysql-protocol/packet/types"

const (
	OKPacketHeader                = 0x00
	EOFPacketHeader               = 0xfe
	ErrPacketHeader               = 0xff
	AuthSwitchRequestPacketHeader = 0xfe
	AuthMoreDataPacketHeader      = 0x01
	LocalInfileRequest            = 0xfb
)

func IsOK(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	payloadLen := types.FixedLengthInteger.Get(data[:3])
	return data[4] == OKPacketHeader && payloadLen >= 7
}

func IsEOF(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	payloadLen := types.FixedLengthInteger.Get(data[:3])
	return data[4] == EOFPacketHeader && payloadLen < 9
}

func IsErr(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return data[4] == ErrPacketHeader
}

func IsAuthSwitchRequest(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return data[4] == AuthSwitchRequestPacketHeader
}

func IsAuthMoreData(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return data[4] == AuthMoreDataPacketHeader
}

func IsLocalInfileRequest(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return data[4] == LocalInfileRequest
}
