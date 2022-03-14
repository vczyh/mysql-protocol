package packet

//https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchResponse

func NewAuthSwitchResponse(authRes []byte) *Simple {
	return NewSimple(authRes)
}

func ParseAuthSwitchResponse(data []byte) ([]byte, error) {
	return data, nil
}

// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData

func ParseAuthMoreData(data []byte) ([]byte, error) {
	pluginData := data[1:]
	return pluginData, nil
}

func NewAuthMoreData(pluginData []byte) Packet {
	return NewSimple(append([]byte{0x01}, pluginData...))
}
