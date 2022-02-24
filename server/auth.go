package server

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/errors"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	mysqlrand "github.com/vczyh/mysql-protocol/rand"
	"os"
)

func (s *server) auth(conn mysql.Conn) error {
	hs, err := s.writeHandshakePacket(conn)
	if err != nil {
		return err
	}

	hsr, err := s.handleTLSAndHandshakeResponse(conn)
	if err != nil {
		return err
	}

	if conn.Closed() {
		return nil
	}

	authData := hs.GetAuthData()
	user := hsr.GetUsername()
	authRes := hsr.AuthRes
	host := s.clientHost(conn)

	errAccessDenied := errors.AccessDenied.Build(user, host, "YES")

	key, err := s.userProvider.Key(user, host)
	if err != nil {
		return err
	}

	method, err := s.userProvider.AuthenticationMethod(key)
	if err != nil {
		// TODO maybe return user not found error
		return err
	}

	if hsr.AuthPlugin != method {
		authData, err = s.writeAuthSwitchRequestPacket(conn, method)
		if err != nil {
			return err
		}
		authRes, err = s.handleAuthSwitchResponsePacket(conn)
		if err != nil {
			return err
		}
	}

	return s.authentication(conn, method, key, authRes, authData, errAccessDenied)
}

func (s *server) writeAuthSwitchRequestPacket(conn mysql.Conn, method core.AuthenticationMethod) ([]byte, error) {
	authData := mysqlrand.Bytes(20)
	return authData, conn.WritePacket(packet.NewAuthSwitchRequest(method, append(authData, 0x00)))
}

func (s *server) handleAuthSwitchResponsePacket(conn mysql.Conn) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}
	return packet.ParseAuthSwitchResponse(data)
}

func (s *server) authentication(conn mysql.Conn, method core.AuthenticationMethod, key string,
	authRes, salt []byte, errAccessDenied error) error {

	switch method {
	case core.MySQLNativePassword:
		as, err := s.userProvider.AuthenticationString(key)
		if err != nil {
			// TODO maybe error is not found
			return err
		}
		if len(as) != 41 {
			// TODO go errors
			return ErrInvalidAuthenticationStringFormat
		}
		challengeRes, err := hex.DecodeString(string(bytes.ToLower(as[1:])))
		if err != nil {
			return err
		}

		if err := method.ChallengeResponse(challengeRes, authRes, salt); err != nil {
			if err == core.ErrMismatch {
				return errAccessDenied
			}
			return err
		}
		return conn.WriteEmptyOK()

	case core.SHA256Password:
		as, err := s.userProvider.AuthenticationString(key)
		if err != nil {
			return err
		}

		password, err := s.sha256Password(conn, authRes, salt)
		if err != nil {
			return err
		}

		if err := method.Validate(as, password); err != nil {
			if err == core.ErrMismatch {
				return errAccessDenied
			}
			return err
		}
		return conn.WriteEmptyOK()

	// https://dev.mysql.com/blog-archive/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
	case core.CachingSha2Password:
		challengeRes := s.sha2Cache.Get(key)
		if challengeRes != nil {
			// Fast authentication
			if err := conn.WritePacket(packet.NewAuthMoreData([]byte{0x03})); err != nil {
				return err
			}

			if err := method.ChallengeResponse(challengeRes, authRes, salt); err != nil {
				if err == core.ErrMismatch {
					return conn.WriteMySQLError(errAccessDenied)
				}
				return err
			}
			return conn.WriteEmptyOK()

		} else {
			// Full authentication
			if err := conn.WritePacket(packet.NewAuthMoreData([]byte{0x04})); err != nil {
				return err
			}

			password, err := s.cachingSHA2Password(conn, salt)
			if err != nil {
				return err
			}

			as, err := s.userProvider.AuthenticationString(key)
			if err != nil {
				return err
			}

			if err := method.Validate(as, password); err != nil {
				if err == core.ErrMismatch {
					return errAccessDenied
				}
				return err
			}
			challengeData, err := method.GenerateChallengeData(password)
			if err != nil {
				return err
			}
			s.sha2Cache.Put(key, challengeData)
			return conn.WriteEmptyOK()
		}

	default:
		return core.ErrUnsupportedAuthenticationMethod
	}
}

func (s *server) sha256Password(conn mysql.Conn, authRes, salt []byte) ([]byte, error) {
	if len(authRes) != 1 || authRes[0] != 0x01 {
		return nil, packet.ErrPacketData
	}
	if err := s.writePublicKeyPacket(conn); err != nil {
		return nil, err
	}

	return s.plaintextPassword(conn, salt)
}

func (s *server) plaintextPassword(conn mysql.Conn, salt []byte) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// TODO private key is nil
	plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, s.privateKey, data[4:], nil)
	if err != nil {
		return nil, err
	}

	for i := range plain {
		j := i % len(salt)
		plain[i] ^= salt[j]
	}
	if len(plain) == 0 {
		return nil, packet.ErrPacketData
	}

	return plain[:len(plain)-1], nil
}

func (s *server) writePublicKeyPacket(conn mysql.Conn) error {
	return conn.WritePacket(packet.NewAuthMoreData(s.publicKeyBytes))
}

func (s *server) cachingSHA2Password(conn mysql.Conn, salt []byte) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	if conn.TLS() {
		// TODO
		fmt.Println("TLS")
		return nil, fmt.Errorf("TLS not finished")
	} else {
		if !packet.IsRequestPublicKey(data) {
			return nil, packet.ErrPacketData
		}

		if err := s.writePublicKeyPacket(conn); err != nil {
			return nil, err
		}

		return s.plaintextPassword(conn, salt)
	}
}

func (s *server) writeHandshakePacket(conn mysql.Conn) (*packet.Handshake, error) {
	salt1 := mysqlrand.Bytes(8)

	hs := &packet.Handshake{
		ProtocolVersion:   0x0a,
		ServerVersion:     s.version,
		ConnectionId:      conn.ConnectionId(),
		Salt1:             salt1,
		CharacterSet:      core.Utf8mb40900AiCi,
		StatusFlags:       core.ServerStatusAutocommit,
		AuthPluginDataLen: 21,
		AuthPlugin:        s.defaultAuthMethod,
	}
	hs.SetCapabilities(conn.Capabilities())

	switch s.defaultAuthMethod {
	case core.MySQLNativePassword:
		hs.Salt2 = mysqlrand.Bytes(13)
	case core.CachingSha2Password, core.SHA256Password:
		hs.Salt2 = append(mysqlrand.Bytes(12), 0x00)
	default:
		return nil, core.ErrUnsupportedAuthenticationMethod
	}

	return hs, conn.WritePacket(hs)
}

func (s *server) handleTLSAndHandshakeResponse(conn mysql.Conn) (*packet.HandshakeResponse, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// SSL request
	if len(data) == 4+4+4+1+23 {
		if err := s.handleTLS(data, conn); err != nil {
			return nil, err
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

func (s *server) buildKeyPair() error {
	if s.privatePath != "" {
		privateBytes, err := os.ReadFile(s.privatePath)
		if err != nil {
			return err
		}

		block, rest := pem.Decode(privateBytes)
		if block == nil {
			return fmt.Errorf("no pem data found, data: %s", rest)
		}

		s.privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return err
		}
	}

	if s.publicPath != "" {
		publicKeyBytes, err := os.ReadFile(s.publicPath)
		if err != nil {
			return err
		}
		s.publicKeyBytes = publicKeyBytes
	}

	return nil
}
