package packet

func NewAuthSwitchResponse(authRes []byte) *Simple {
	return NewSimple(authRes)
}

func ParseAuthSwitchResponse(data []byte) ([]byte, error) {
	if len(data) < 5 {
		return nil, ErrPacketData
	}
	return data[4:], nil
}

func ParseAuthMoreData(data []byte) ([]byte, error) {
	if len(data) < 6 {
		return nil, ErrPacketData
	}
	pluginData := data[5:]
	return pluginData, nil
}

func NewAuthMoreData(pluginData []byte) Packet {
	return NewSimple(append([]byte{0x01}, pluginData...))
}
