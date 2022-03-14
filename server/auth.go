package server

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/charset"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/myerrors"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"net"
	"os"
	"path"
)

var (
	ErrPrivateKeyNotFond = errors.New("auth: private key not found")
)

const (
	PrivateKeyName = "private_key.pem"
	PublicKeyName  = "public_key.pem"
)

func (s *server) auth(conn mysql.Conn) error {
	hs, err := s.writeHandshakePacket(conn)
	if err != nil {
		return err
	}

	hsr, err := s.handleTLSAndHandshakeResponsePacket(conn)
	if err != nil {
		return err
	}

	authData := hs.GetAuthData()
	user := hsr.GetUsername()
	authRes := hsr.AuthRes

	var host string
	switch v := conn.RemoteAddr().(type) {
	case *net.TCPAddr:
		host = v.IP.String()
	}

	errAccessDenied := myerrors.AccessDenied.Build(user, host, "YES")

	key, err := s.config.UserProvider.Key(user, host)
	if err != nil {
		if err == ErrAccessDenied {
			return errAccessDenied
		}
		return err
	}

	method, err := s.config.UserProvider.AuthenticationMethod(key)
	if err != nil {
		if err == ErrAccessDenied {
			return errAccessDenied
		}
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

	if err := s.authentication(conn, method, key, authRes, authData, errAccessDenied); err != nil {
		return err
	}

	err = s.config.UserProvider.Authorization(key, &AuthorizationRequest{
		Database: func() string {
			if conn.Capabilities()&flag.ClientConnectWithDB != 0 {
				return hsr.GetDatabase()
			}
			return ""
		}(),
		TLSed: conn.TLSed(),
	})
	if err != nil {
		if err == ErrAccessDenied {
			return errAccessDenied
		}
		return err
	}

	return nil
}

func (s *server) writeAuthSwitchRequestPacket(conn mysql.Conn, method auth.Method) ([]byte, error) {
	authData := auth.Bytes(20)
	return authData, conn.WritePacket(packet.NewAuthSwitchRequest(method, append(authData, 0x00)))
}

func (s *server) handleAuthSwitchResponsePacket(conn mysql.Conn) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}
	return packet.ParseAuthSwitchResponse(data)
}

func (s *server) authentication(conn mysql.Conn, method auth.Method, key string,
	authRes, salt []byte, errAccessDenied error) error {

	switch method {
	case auth.MySQLNativePassword:
		as, err := s.config.UserProvider.AuthenticationString(key)
		if err != nil {
			if err == ErrAccessDenied {
				return errAccessDenied
			}
			return err
		}
		if len(as) != 41 {
			return ErrInvalidAuthenticationStringFormat
		}
		challengeData, err := hex.DecodeString(string(bytes.ToLower(as[1:])))
		if err != nil {
			return err
		}

		if err := method.ChallengeResponse(challengeData, authRes, salt); err != nil {
			if err == auth.ErrMismatch {
				return errAccessDenied
			}
			return err
		}

	case auth.SHA256Password:
		as, err := s.config.UserProvider.AuthenticationString(key)
		if err != nil {
			if err == ErrAccessDenied {
				return errAccessDenied
			}
			return err
		}

		password, err := s.sha256Password(conn, authRes, salt)
		if err != nil {
			return err
		}

		if err := method.ReAscertainPassword(as, password); err != nil {
			if err == auth.ErrMismatch {
				return errAccessDenied
			}
			return err
		}

	// https://dev.mysql.com/blog-archive/preparing-your-community-connector-for-mysql-8-part-2-sha256/
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
	case auth.CachingSha2Password:
		challengeData := s.config.SHA2Cache.Get(key)
		if challengeData != nil {
			// fast authentication
			if err := conn.WritePacket(packet.NewAuthMoreData([]byte{0x03})); err != nil {
				return err
			}

			if err := method.ChallengeResponse(challengeData, authRes, salt); err != nil {
				if err == auth.ErrMismatch {
					return conn.WriteError(errAccessDenied)
				}
				return err
			}
		} else {
			// full authentication
			if err := conn.WritePacket(packet.NewAuthMoreData([]byte{0x04})); err != nil {
				return err
			}

			password, err := s.cachingSHA2Password(conn, salt)
			if err != nil {
				return err
			}
			as, err := s.config.UserProvider.AuthenticationString(key)
			if err != nil {
				return err
			}
			if err := method.ReAscertainPassword(as, password); err != nil {
				if err == auth.ErrMismatch {
					return errAccessDenied
				}
				return err
			}

			challengeData, err := method.GenerateChallengeData(password)
			if err != nil {
				return err
			}
			s.config.SHA2Cache.Put(key, challengeData)
		}

	default:
		return auth.ErrUnsupportedAuthenticationMethod
	}

	return nil
}

func (s *server) writePublicKeyPacket(conn mysql.Conn, publicKeyBytes []byte) error {
	if len(publicKeyBytes) == 0 {
		return myerrors.NewServer(code.ErrSendToClient, "public key not setting")
	}
	return conn.WritePacket(packet.NewAuthMoreData(publicKeyBytes))
}

func (s *server) sha256Password(conn mysql.Conn, authRes, salt []byte) ([]byte, error) {
	if conn.TLSed() {
		if len(authRes) == 0 {
			return nil, nil
		}
		return authRes[:len(authRes)-1], nil
	}

	if len(authRes) != 1 || authRes[0] != 0x01 {
		return nil, packet.ErrPacketData
	}
	if err := s.writePublicKeyPacket(conn, s.sha256PasswordPublicKeyBytes); err != nil {
		return nil, err
	}

	return s.plaintextPassword(conn, s.sha256PasswordPrivateKey, salt)
}

func (s *server) cachingSHA2Password(conn mysql.Conn, salt []byte) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	if conn.TLSed() {
		authRes := data[4:]
		if len(authRes) == 0 {
			return nil, nil
		}
		return authRes[:len(authRes)-1], nil
	} else {
		if !packet.IsRequestPublicKey(data) {
			return nil, packet.ErrPacketData
		}

		if err := s.writePublicKeyPacket(conn, s.cachingSHA2PasswordPublicKeyBytes); err != nil {
			return nil, err
		}

		return s.plaintextPassword(conn, s.cachingSHA2PasswordPrivateKey, salt)
	}
}

func (s *server) plaintextPassword(conn mysql.Conn, privateKey *rsa.PrivateKey, salt []byte) ([]byte, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	if s.privateKey == nil {
		return nil, ErrPrivateKeyNotFond
	}
	plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, privateKey, data, nil)
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

func (s *server) writeHandshakePacket(conn mysql.Conn) (*packet.Handshake, error) {
	salt1 := auth.Bytes(8)

	hs := &packet.Handshake{
		ProtocolVersion:   0x0a,
		ServerVersion:     s.config.Version,
		ConnectionId:      conn.ConnectionId(),
		Salt1:             salt1,
		CharacterSet:      charset.Utf8mb40900AiCi,
		StatusFlags:       flag.ServerStatusAutocommit,
		AuthPluginDataLen: 21,
		AuthPlugin:        s.config.DefaultAuthMethod,
	}
	hs.SetCapabilities(conn.Capabilities())

	switch s.config.DefaultAuthMethod {
	case auth.MySQLNativePassword:
		hs.Salt2 = auth.Bytes(13)
	case auth.CachingSha2Password, auth.SHA256Password:
		hs.Salt2 = append(auth.Bytes(12), 0x00)
	default:
		return nil, auth.ErrUnsupportedAuthenticationMethod
	}

	return hs, conn.WritePacket(hs)
}

func (s *server) handleTLSAndHandshakeResponsePacket(conn mysql.Conn) (*packet.HandshakeResponse, error) {
	data, err := conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	// SSL request
	if len(data) == 4+4+4+1+23 {
		if err := s.handleTLSPacket(data, conn); err != nil {
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

func (s *server) readSHA256PasswordKeyPair() (err error) {
	privateKeyPath := s.config.SHA256PasswordPrivateKeyPath
	publicKeyPath := s.config.SHA256PasswordPublicKeyPath

	if privateKeyPath == "" || publicKeyPath == "" {
		s.sha256PasswordPrivateKey = s.privateKey
		s.sha256PasswordPublicKeyBytes = s.publicKeyBytes
		return nil
	}

	s.sha256PasswordPrivateKey,
		s.sha256PasswordPublicKeyBytes,
		err = readKeyPair(privateKeyPath, publicKeyPath)
	return err
}

func (s *server) readCachingSHA2PasswordKeyPair() (err error) {
	privateKeyPath := s.config.CachingSHA2PasswordPrivateKeyPath
	publicKeyPath := s.config.CachingSHA2PasswordPublicKeyPath

	if privateKeyPath == "" || publicKeyPath == "" {
		s.cachingSHA2PasswordPrivateKey = s.privateKey
		s.cachingSHA2PasswordPublicKeyBytes = s.publicKeyBytes
		return nil
	}

	s.cachingSHA2PasswordPrivateKey,
		s.cachingSHA2PasswordPublicKeyBytes,
		err = readKeyPair(privateKeyPath, publicKeyPath)
	return err
}

func (s *server) generateReadKeyPair() (err error) {
	dir := s.config.RSAKeysDir
	privateKeyPath := path.Join(dir, PrivateKeyName)
	publicKeyPath := path.Join(dir, PublicKeyName)

	// If private/public key-pair have been existed in local file, will read them
	// and not generate key-pair.
	isExist, err := s.isRSAKeysExist()
	if err != nil {
		return err
	}
	if isExist {
		s.privateKey, s.publicKeyBytes, err = readKeyPair(privateKeyPath, publicKeyPath)
		return err
	}

	// Generate private/public key-pair.
	s.privateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	publicKeyDerBytes, err := x509.MarshalPKIXPublicKey(&s.privateKey.PublicKey)
	if err != nil {
		return err
	}
	if s.publicKeyBytes, err = encodePEM(&pem.Block{Type: "PUBLIC KEY", Bytes: publicKeyDerBytes}); err != nil {
		return err
	}

	if s.config.RSAKeysDir == "" {
		return nil
	}

	// Write private/public key-pair to file.
	return writeKeyPair(s.privateKey, privateKeyPath, publicKeyPath)
}

func (s *server) isRSAKeysExist() (bool, error) {
	dir := s.config.RSAKeysDir
	if dir == "" {
		return false, nil
	}

	isExistFunc := func(name string) (bool, error) {
		_, err := os.Stat(path.Join(dir, name))
		if err != nil {
			if os.IsNotExist(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	isPrivateKeyExist, err := isExistFunc(PrivateKeyName)
	if err != nil {
		return false, err
	}
	isPublicKeyExist, err := isExistFunc(PublicKeyName)
	if err != nil {
		return false, err
	}

	if isPrivateKeyExist || isPublicKeyExist {
		return true, nil
	}

	return false, nil
}

func readKeyPair(privateKeyPath, publicKeyPath string) (privateKey *rsa.PrivateKey,
	publicKeyBytes []byte, err error) {

	privateBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, nil, err
	}
	block, rest := pem.Decode(privateBytes)
	if block == nil {
		return nil, nil, fmt.Errorf("no pem data found, data: %s", rest)
	}

	privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		privateKeyGeneric, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, nil, err
		}
		privateKey = privateKeyGeneric.(*rsa.PrivateKey)
	}

	publicKeyBytes, err = os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, nil, err
	}

	return privateKey, publicKeyBytes, nil
}

func writeKeyPair(key *rsa.PrivateKey, keyFile, pubFile string) error {
	keyDerBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := writePEMFile(true, keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDerBytes}); err != nil {
		return err
	}

	pubDerBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	return writePEMFile(false, pubFile, &pem.Block{Type: "PUBLIC KEY", Bytes: pubDerBytes})
}

func encodePEM(b *pem.Block) ([]byte, error) {
	var buf bytes.Buffer
	if err := pem.Encode(&buf, b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writePEMFile(isPrivateKey bool, pemFile string, block *pem.Block) error {
	out, err := openPEMFile(isPrivateKey, pemFile)
	if err != nil {
		return err
	}
	defer out.Close()
	return pem.Encode(out, block)
}

func openPEMFile(isPrivateKey bool, filename string) (*os.File, error) {
	if err := mkdirParent(filename); err != nil {
		return nil, err
	}

	if isPrivateKey {
		return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	}
	return os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
}

func mkdirParent(filename string) error {
	return os.MkdirAll(path.Dir(filename), 0751)
}
