package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/flag"
)

type AuthSwitchRequest struct {
	PayloadHeader uint8 // 0xfe
	AuthPlugin    auth.Method
	AuthData      []byte
}

func NewAuthSwitchRequest(method auth.Method, authData []byte) *AuthSwitchRequest {
	return &AuthSwitchRequest{
		PayloadHeader: 0xfe,
		AuthPlugin:    method,
		AuthData:      authData,
	}
}

func ParseAuthSwitchRequest(data []byte) (*AuthSwitchRequest, error) {
	p := new(AuthSwitchRequest)

	buf := bytes.NewBuffer(data)
	p.PayloadHeader = uint8(FixedLengthInteger.Get(buf.Next(1)))

	pluginName, err := NulTerminatedString.Get(buf)
	if err != nil {
		return nil, err
	}
	if p.AuthPlugin, err = auth.ParseAuthenticationPlugin(string(pluginName)); err != nil {
		return nil, err
	}

	p.AuthData = buf.Bytes()

	return p, nil
}

func (p *AuthSwitchRequest) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer

	payload.WriteByte(p.PayloadHeader)
	payload.Write(NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
	payload.Write(p.AuthData)

	return payload.Bytes(), nil
}
