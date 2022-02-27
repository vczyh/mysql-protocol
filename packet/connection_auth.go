package packet

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/flag"
)

type AuthSwitchRequest struct {
	Header

	PayloadHeader uint8
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
	if p.AuthPlugin, err = auth.ParseAuthenticationPlugin(string(pluginName)); err != nil {
		return nil, err
	}

	p.AuthData = buf.Bytes()

	return &p, nil
}

func (p *AuthSwitchRequest) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer

	payload.WriteByte(p.PayloadHeader)
	payload.Write(NulTerminatedString.Dump([]byte(p.AuthPlugin.String())))
	payload.Write(p.AuthData)

	p.Length = uint32(payload.Len())

	dump := make([]byte, 3+1+p.Length)
	headerDump, err := p.Header.Dump(capabilities)
	if err != nil {
		return nil, err
	}
	copy(dump, headerDump)
	copy(dump[4:], payload.Bytes())

	return dump, nil
}
