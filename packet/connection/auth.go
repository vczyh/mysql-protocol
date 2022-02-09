package connection

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
)

type AuthSwitchRequest struct {
	generic.Header

	PayloadHeader uint8
	AuthPlugin    generic.AuthenticationPlugin
	AuthData      []byte
}

func ParseAuthSwitchRequest(data []byte) (*AuthSwitchRequest, error) {
	var p AuthSwitchRequest
	var err error

	buf := bytes.NewBuffer(data)
	if err = p.Header.Parse(buf); err != nil {
		return nil, err
	}

	p.PayloadHeader = uint8(types.FixedLengthInteger.Get(buf.Next(1)))

	pluginName, err := types.NulTerminatedString.Get(buf)
	if err != nil {
		return nil, err
	}
	if p.AuthPlugin, err = generic.ParseAuthenticationPlugin(string(pluginName)); err != nil {
		return nil, err
	}

	p.AuthData = buf.Bytes()

	return &p, nil
}
