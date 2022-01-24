package client

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"mysql-protocol/packet/connection"
	"mysql-protocol/packet/generic"
)

func (c *Conn) auth(plugin connection.AuthenticationPlugin, authData []byte) error {
	data, err := c.ReadPacket()
	if err != nil {
		return err
	}

	var authSwitch bool
	switch {
	case generic.IsOK(data) || generic.IsErr(data):
		return c.HandleOKErrPacket(data)
	case generic.IsAuthSwitchRequest(data):
		switchPkt, err := connection.ParseAuthSwitchRequest(data)
		if err != nil {
			return err
		}
		authSwitch = true
		plugin = switchPkt.GetPlugin()
		authData = switchPkt.AuthData
	}

	// auth switch response
	if authSwitch {
		if err = c.writeAuthSwitchResponsePacket(plugin, authData); err != nil {
			return err
		}
		if data, err = c.ReadPacket(); err != nil {
			return err
		}
	}

	switch {
	case generic.IsOK(data) || generic.IsErr(data):
		return c.HandleOKErrPacket(data)
	case generic.IsAuthMoreData(data):
		pluginData, err := connection.ParseAuthMoreData(data)
		if err != nil {
			return err
		}
		switch plugin {
		// https://dev.mysql.com/blog-archive/preparing-your-community-connector-for-mysql-8-part-2-sha256/
		// https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
		case connection.CachingSHA2Password:
			switch pluginData[0] {
			// fast authentication
			case 0x03:
				return c.ReadOKErrPacket()
			// full authentication
			case 0x04:
				// TODO if TLS
				// request public key
				pubKeyData, err := c.requestPublicKey()
				if err != nil {
					return err
				}
				// send encrypted password
				if err := c.writePasswordEncryptedWithPublicKeyPacket(pubKeyData, authData); err != nil {
					return err
				}
				return c.ReadOKErrPacket()
			}
		}
	}

	return generic.ErrPacketData
}

func (c *Conn) writeAuthSwitchResponsePacket(plugin connection.AuthenticationPlugin, authData []byte) (err error) {
	encryptedPassword, err := connection.EncryptPassword(plugin, []byte(c.password), authData)
	authRes := connection.NewAuthSwitchResponse(encryptedPassword)
	return c.WritePacket(authRes)

	//var switchResPkt connection.AuthSwitchResponse
	//switchResPkt.AuthRes, err = connection.EncryptPassword(plugin, []byte(c.password), authData)
	//if err != nil {
	//	return err
	//}
	//return c.writePacket(&switchResPkt)
}

func (c *Conn) requestPublicKey() ([]byte, error) {
	simplePkt := generic.NewSimple([]byte{0x02})
	if err := c.WritePacket(simplePkt); err != nil {
		return nil, err
	}

	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	pluginData, err := connection.ParseAuthMoreData(data)
	if err != nil {
		return nil, err
	}

	return pluginData, nil
}

func (c *Conn) writePasswordEncryptedWithPublicKeyPacket(data []byte, seed []byte) error {
	block, rest := pem.Decode(data)
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

	simplePkt := generic.NewSimple(encryptedPassword)
	return c.WritePacket(simplePkt)
}
