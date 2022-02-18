package server

import (
	"bytes"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/errors"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
)

func (s *server) auth(conn mysql.Conn) error {
	hs, err := s.writeHandshakePacket(conn)
	if err != nil {
		return err
	}

	hsr, err := s.handleSSLAndHandshakeResponse(conn)
	if err != nil {
		return err
	}

	if conn.Closed() {
		return nil
	}

	plugin := hs.AuthPlugin
	if hsr.AuthPlugin != plugin {
		// TODO send auth switch
		// https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
		return fmt.Errorf("unsupported auth plugin: %s", hsr.AuthPlugin)
	}

	switch plugin {
	case core.MySQLNativePasswordPlugin:
		passwordEncrypted, err := core.EncryptPassword(core.MySQLNativePasswordPlugin, []byte(s.password), hs.GetAuthData())
		if err != nil {
			return err
		}
		if hsr.GetUsername() != s.user || !bytes.Equal(hsr.AuthRes, passwordEncrypted) {
			if err := conn.WriteError(errors.AccessDenied.Err(hsr.GetUsername(), "%", "YES")); err != nil {
				return err
			}
			s.closeConnection(conn)
			return nil
		}
	case core.CachingSHA2PasswordPlugin:
		// TODO
		return fmt.Errorf("unsupported auth plugin")
	}

	// successful authentication
	if err := conn.WriteEmptyOK(); err != nil {
		return err
	}
	s.h.OnConnect(conn.ConnectionId())

	return nil
}

func (s *server) writeHandshakePacket(conn mysql.Conn) (*packet.Handshake, error) {
	salt1 := RandBytes(8)

	hs := &packet.Handshake{
		ProtocolVersion:   0x0a,
		ServerVersion:     s.version,
		ConnectionId:      conn.ConnectionId(),
		Salt1:             salt1,
		CharacterSet:      core.Utf8mb40900AiCi,
		StatusFlags:       core.ServerStatusAutocommit,
		AuthPluginDataLen: 21,
		AuthPlugin:        s.plugin,
	}
	hs.SetCapabilities(conn.Capabilities())

	switch s.plugin {
	case core.MySQLNativePasswordPlugin:
		hs.Salt2 = RandBytes(13)
	case core.CachingSHA2PasswordPlugin:
		hs.Salt2 = append(RandBytes(12), 0x00)
	default:
		return nil, fmt.Errorf("unsupported plugin: %s", s.plugin)
	}

	return hs, conn.WritePacket(hs)
}

func (s *server) handleSSLAndHandshakeResponse(conn mysql.Conn) (*packet.HandshakeResponse, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// SSL request
	if len(data) == 4+4+4+1+23 {
		if err := s.handleSSL(data, conn); err != nil {
			return nil, err
		}
		if conn.Closed() {
			return nil, nil
		}
		if data, err = conn.ReadPacket(); err != nil {
			return nil, err
		}
	}

	hs, err := packet.ParseHandshakeResponse(data)
	if err != nil {
		return nil, err
	}

	// TODO update capabilities
	conn.SetCapabilities(hs.ClientCapabilityFlags)

	return hs, nil
}
