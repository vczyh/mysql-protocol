package packet

const (
	OKPacketHeader                = 0x00
	EOFPacketHeader               = 0xfe
	ErrPacketHeader               = 0xff
	AuthSwitchRequestPacketHeader = 0xfe
	AuthMoreDataPacketHeader      = 0x01
	LocalInfileRequestHeader      = 0xfb
	RequestPublicKeyHeader        = 0x02
)

func IsOK(data []byte) bool {
	return data[0] == OKPacketHeader && len(data) >= 7
}

func IsEOF(data []byte) bool {
	return data[0] == EOFPacketHeader && len(data) < 9
}

func IsErr(data []byte) bool {
	return data[0] == ErrPacketHeader
}

func IsAuthSwitchRequest(data []byte) bool {
	return data[0] == AuthSwitchRequestPacketHeader
}

func IsAuthMoreData(data []byte) bool {
	return data[0] == AuthMoreDataPacketHeader
}

func IsLocalInfileRequest(data []byte) bool {
	return data[0] == LocalInfileRequestHeader
}

func IsRequestPublicKey(data []byte) bool {
	return data[0] == RequestPublicKeyHeader
}

func IsPing(data []byte) bool {
	return len(data) == 1 && data[0] == ComPing.Byte()
}

func IsQuery(data []byte) bool {
	return data[0] == ComQuery.Byte()
}

func IsQuit(data []byte) bool {
	return len(data) == 1 && data[0] == ComQuit.Byte()
}
