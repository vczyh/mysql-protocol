package connection

import (
	"github.com/vczyh/mysql-protocol/packet/generic"
)

func NewAuthSwitchResponse(authRes []byte) *generic.Simple {
	return generic.NewSimple(authRes)
}

func ParseAuthMoreData(data []byte) ([]byte, error) {
	if len(data) < 6 {
		return nil, generic.ErrPacketData
	}
	pluginData := data[5:]
	return pluginData, nil
}
