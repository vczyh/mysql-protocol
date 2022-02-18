package packet

func NewAuthSwitchResponse(authRes []byte) *Simple {
	return NewSimple(authRes)
}

func ParseAuthMoreData(data []byte) ([]byte, error) {
	if len(data) < 6 {
		return nil, ErrPacketData
	}
	pluginData := data[5:]
	return pluginData, nil
}
