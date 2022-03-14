package client

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/packet"
)

func (c *conn) auth(method auth.Method, authData []byte) error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}

	if packet.IsAuthSwitchRequest(data) {
		return c.handleAuthSwitchRequestPacket(data)
	}
	return c.finalAuth(method, data, authData)
}

func (c *conn) handleAuthSwitchRequestPacket(data []byte) error {
	switchPkt, err := packet.ParseAuthSwitchRequest(data)
	if err != nil {
		return err
	}

	method := switchPkt.AuthPlugin
	authData := switchPkt.AuthData[:len(switchPkt.AuthData)-1]
	if err = c.writeAuthSwitchResponsePacket(method, authData); err != nil {
		return err
	}

	data, err = c.ReadPacket()
	if err != nil {
		return err
	}
	return c.finalAuth(method, data, authData)
}

func (c *conn) finalAuth(method auth.Method, data, authData []byte) error {
	switch method {
	case auth.MySQLNativePassword:
		return c.handleOKERRPacket(data)
	case auth.SHA256Password:
		return c.sha256Authentication(data, authData)
	case auth.CachingSha2Password:
		return c.cachingSHA2Authentication(data, authData)
	default:
		return auth.ErrUnsupportedAuthenticationMethod
	}
}

func (c *conn) writeAuthSwitchResponsePacket(method auth.Method, authData []byte) (err error) {
	authRes, err := c.generateAuthRes(method, authData)
	if err != nil {
		return err
	}
	return c.WritePacket(packet.NewAuthSwitchResponse(authRes))
}

func (c *conn) generateAuthRes(method auth.Method, authData []byte) (authRes []byte, err error) {
	switch method {
	case auth.MySQLNativePassword, auth.CachingSha2Password:
		if c.password == "" {
			return nil, nil
		}
		return method.EncryptPassword([]byte(c.password), authData)

	case auth.SHA256Password:
		if c.password == "" {
			return []byte{0x00}, nil
		}
		if c.mysqlConn.TLSed() {
			return append([]byte(c.password), 0x00), nil
		}
		// request public key from server
		return []byte{0x01}, nil

	default:
		return nil, auth.ErrUnsupportedAuthenticationMethod
	}
}

func (c *conn) sha256Authentication(data, authData []byte) error {
	pluginData, err := packet.ParseAuthMoreData(data)
	if err != nil {
		return err
	}
	if err := c.writePasswordEncryptedWithPublicKeyPacket(pluginData, authData); err != nil {
		return err
	}
	return c.readOKERRPacket()
}

func (c *conn) cachingSHA2Authentication(data, authData []byte) error {
	switch {
	case packet.IsOK(data) || packet.IsErr(data):
		return c.handleOKERRPacket(data)
	case packet.IsAuthMoreData(data):
		pluginData, err := packet.ParseAuthMoreData(data)
		if err != nil {
			return err
		}
		// https://dev.mysql.com/blog-archive/preparing-your-community-connector-for-mysql-8-part-2-sha256/
		// https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
		switch pluginData[0] {
		// fast authentication
		case 0x03:
			return c.readOKERRPacket()
		// full authentication
		case 0x04:
			// TODO if TLSed
			// request public key
			pubKeyData, err := c.requestPublicKey()
			if err != nil {
				return err
			}
			// send encrypted password
			if err := c.writePasswordEncryptedWithPublicKeyPacket(pubKeyData, authData); err != nil {
				return err
			}
			return c.readOKERRPacket()
		}
	}

	return packet.ErrPacketData
}

func (c *conn) requestPublicKey() ([]byte, error) {
	simplePkt := packet.NewSimple([]byte{0x02})
	if err := c.WritePacket(simplePkt); err != nil {
		return nil, err
	}

	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	pluginData, err := packet.ParseAuthMoreData(data)
	if err != nil {
		return nil, err
	}

	return pluginData, nil
}

func (c *conn) writePasswordEncryptedWithPublicKeyPacket(pubBytes []byte, seed []byte) error {
	block, rest := pem.Decode(pubBytes)
	if block == nil {
		return fmt.Errorf("no pem data found, data: %s", rest)
	}
	pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return err
	}

	plain := make([]byte, len(c.password)+1)
	copy(plain, c.password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}

	h := sha1.New()
	encryptedPassword, err := rsa.EncryptOAEP(h, rand.Reader, pkix.(*rsa.PublicKey), plain, nil)
	if err != nil {
		return err
	}

	simplePkt := packet.NewSimple(encryptedPassword)
	return c.WritePacket(simplePkt)
}
