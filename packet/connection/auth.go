package connection

import (
	"bytes"
	"mysql-protocol/packet/generic"
	"mysql-protocol/packet/types"
)

type AuthSwitchRequest struct {
	generic.Header

	PayloadHeader uint8
	PluginName    []byte
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
	if p.PluginName, err = types.NulTerminatedString.Get(buf); err != nil {
		return nil, err
	}
	p.AuthData = buf.Bytes()

	return &p, nil
}

func (p *AuthSwitchRequest) GetPlugin() AuthenticationPlugin {
	switch string(p.PluginName) {
	case MySQLNativePassword.String():
		return MySQLNativePassword
	case CachingSHA2Password.String():
		return CachingSHA2Password
	default:
		return MySQLNativePassword
	}
}
