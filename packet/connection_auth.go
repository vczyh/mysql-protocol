package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/core"
)

type AuthSwitchRequest struct {
	Header

	PayloadHeader uint8
	AuthPlugin    core.AuthenticationPlugin
	AuthData      []byte
}

func ParseAuthSwitchRequest(data []byte) (*AuthSwitchRequest, error) {
	var p AuthSwitchRequest
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.PayloadHeader = uint8(FixedLengthInteger.Get(buf.Next(1)))

	pluginName, err := NulTerminatedString.Get(buf)
	if err != nil {
		return nil, err
	}
	if p.AuthPlugin, err = core.ParseAuthenticationPlugin(string(pluginName)); err != nil {
		return nil, err
	}

	p.AuthData = buf.Bytes()

	return &p, nil
}
