package server

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet/connection"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"math/big"
)

func (s *server) auth(conn mysql.Conn) error {
	// apply for connection id
	bigN, err := rand.Int(rand.Reader, big.NewInt(2<<32))
	if err != nil {
		return err
	}
	connId := uint32(bigN.Uint64())

	// status flag
	var status generic.StatusFlag = generic.ServerStatusAutocommit

	hs := &connection.Handshake{
		ProtocolVersion: 0x0a,
		// TODO
		ServerVersion: "8.0.27",
		ConnectionId:  connId,
		// TODO random generate
		Salt1:             []byte("12345678"),
		CharacterSet:      generic.Utf8mb40900AiCi,
		StatusFlags:       status,
		AuthPluginDataLen: 21,
		// TODO random generate
		Salt2: []byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x00},
		// TODO other plugin
		AuthPlugin: generic.MySQLNativePasswordPlugin,
		//AuthPlugin: generic.CachingSHA2PasswordPlugin,
	}
	hs.SetCapabilities(conn.Capabilities())
	if err := conn.WritePacket(hs); err != nil {
		return err
	}

	data, err := conn.ReadPacket()
	if err != nil {
		return err
	}
	hsr, err := connection.ParseHandshakeResponse(data)
	if err != nil {
		return err
	}

	// TODO update capabilities

	if hsr.AuthPlugin != hs.AuthPlugin {
		// TODO send auth switch
		// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
		return fmt.Errorf("unsupported auth plugin")
	}

	switch hsr.AuthPlugin {
	case generic.MySQLNativePasswordPlugin:
		passwordEncrypted, err := generic.EncryptPassword(generic.MySQLNativePasswordPlugin, []byte(s.password), hs.GetAuthData())
		if err != nil {
			return err
		}
		if hsr.GetUsername() != s.user || !bytes.Equal(hsr.AuthRes, passwordEncrypted) {
			e := mysql.ErrAccessDenied.Err(hsr.GetUsername(), "%", "YES")
			if err := conn.WritePacket(e.Packet()); err != nil {
				return err
			}
			conn.Close()
			return nil
		}
	}

	if err := conn.WriteEmptyOK(); err != nil {
		return err
	}

	return nil
}
