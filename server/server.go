package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/myerrors"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/mysqllog"
	"github.com/vczyh/mysql-protocol/packet"
	"math/big"
	"net"
	"os"
)

type server struct {
	config    *Config
	tlsConfig *tls.Config

	privateKey     *rsa.PrivateKey
	publicKeyBytes []byte

	cachingSHA2PasswordPrivateKey     *rsa.PrivateKey
	cachingSHA2PasswordPublicKeyBytes []byte

	sha256PasswordPrivateKey     *rsa.PrivateKey
	sha256PasswordPublicKeyBytes []byte

	caCert     tls.Certificate
	serverCert tls.Certificate
	clientCert tls.Certificate

	l net.Listener
}

func NewServer(userProvider UserProvider, handler Handler, opts ...Option) *server {
	s := new(server)
	s.config = &Config{
		UserProvider: userProvider,
		Handler:      handler,
	}

	for _, opt := range opts {
		opt.apply(s)
	}

	return s
}

func (s *server) Start() error {
	if err := s.build(); err != nil {
		s.config.Logger.Error(err)
		return err
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return err
	}
	defer l.Close()
	s.l = l

	for {
		conn, err := s.l.Accept()
		if err != nil {
			s.config.Logger.Error(fmt.Errorf("tcp accept failed: %v", err))
			continue
		}

		connId, err := s.applyForConnectionId()
		if err != nil {
			s.config.Logger.Error(fmt.Errorf("apply for connection id failed: %v", err))
			continue
		}
		go s.handleConnection(mysql.NewServerConnection(conn, connId, s.defaultCapabilities()))
	}
}

func (s *server) build() error {
	if s.config.SHA2Cache == nil {
		s.config.SHA2Cache = NewDefaultSHA2Cache()
	}

	if s.config.UserProvider == nil {
		return fmt.Errorf("require UserProvider not nil")
	}

	if s.config.Handler == nil {
		return fmt.Errorf("require Handler not nil")
	}

	if s.config.Logger == nil {
		s.config.Logger = mysqllog.NewDefaultLogger(mysqllog.SystemLevel, os.Stdout)
	}

	if err := s.generateReadKeyPair(); err != nil {
		return err
	}

	if err := s.readSHA256PasswordKeyPair(); err != nil {
		return err
	}

	if err := s.readCachingSHA2PasswordKeyPair(); err != nil {
		return err
	}

	if err := s.generateReadCerts(); err != nil {
		return err
	}

	if err := s.buildTLSConfig(); err != nil {
		return err
	}

	return nil
}

func (s *server) handleConnection(conn mysql.Conn) {
	defer s.closeConnection(conn)

	if err := s.auth(conn); err != nil {
		if !myerrors.Is(err) {
			s.config.Logger.Error(fmt.Errorf("auth error: %v", err))
		}
		if err := conn.WriteError(err); err != nil {
			s.config.Logger.Error(fmt.Errorf("write error packet failed: %v", err))
		}
		return
	}

	if err := conn.WriteEmptyOK(); err != nil {
		s.config.Logger.Error(fmt.Errorf("write empty ok packet failed: %v", err))
		return
	}

	for {
		if conn.Closed() {
			return
		}
		if err := s.handleCommand(conn); err != nil {
			s.config.Logger.Error(fmt.Errorf("can't handle command error: %v, so close the connection", err))
			return
		}
	}
}

func (s *server) handleCommand(conn mysql.Conn) error {
	data, err := conn.ReadPacket()
	if err != nil {
		return err
	}

	switch {
	case packet.IsPing(data):
		err := s.config.Handler.Ping()
		if err == nil {
			err = conn.WriteEmptyOK()
		} else {
			err = conn.WriteError(err)
		}

	case packet.IsQuery(data):
		rs, err := s.config.Handler.Query(string(data[5:]))
		if err != nil {
			err = conn.WriteError(err)
			break
		}
		err = rs.WriteText(conn)

	case packet.IsQuit(data):
		s.closeConnection(conn)
		s.config.Handler.Quit()

	default:
		s.config.Handler.Other(data, conn)
	}

	return err
}

func (s *server) defaultCapabilities() flag.Capability {
	capabilities := flag.ClientLongPassword |
		flag.ClientFoundRows |
		flag.ClientLongFlag |
		flag.ClientConnectWithDB |
		flag.ClientNoSchema |
		flag.ClientCompress |
		flag.ClientODBC |
		flag.ClientLocalFiles |
		flag.ClientIgnoreSpace |
		flag.ClientProtocol41 |
		flag.ClientInteractive |
		flag.ClientIgnoreSigpipe |
		flag.ClientTransactions |
		flag.ClientSecureConnection |
		flag.ClientMultiStatements |
		flag.ClientMultiResults |
		flag.ClientPsMultiResults |
		flag.ClientPluginAuth |
		flag.ClientConnectAttrs |
		flag.ClientPluginAuthLenencClientData |
		flag.ClientCanHandleExpiredPasswords
	//generic.ClientSessionTrack |
	//generic.ClientDeprecateEOF

	if s.config.UseSSL {
		capabilities |= flag.ClientSSL
	}

	return capabilities
}

func (s *server) applyForConnectionId() (uint32, error) {
	bigN, err := rand.Int(rand.Reader, big.NewInt(2<<32))
	if err != nil {
		return 0, err
	}
	return uint32(bigN.Uint64()), nil
}

func (s *server) closeConnection(conn mysql.Conn) {
	if conn.Closed() {
		return
	}
	conn.Close()
	s.config.Handler.OnClose(conn.ConnectionId())
}

func WithPort(port int) Option {
	return optionFun(func(s *server) {
		s.config.Port = port
	})
}

func WithVersion(version string) Option {
	return optionFun(func(s *server) {
		s.config.Version = version
	})
}

func WithDefaultAuthMethod(method auth.Method) Option {
	return optionFun(func(s *server) {
		s.config.DefaultAuthMethod = method
	})
}

func WithUserProvider(userProvider UserProvider) Option {
	return optionFun(func(s *server) {
		s.config.UserProvider = userProvider
	})
}

func WithSHA2Cache(cache SHA2Cache) Option {
	return optionFun(func(s *server) {
		s.config.SHA2Cache = cache
	})
}

func WithLogger(logger mysqllog.Logger) Option {
	return optionFun(func(s *server) {
		s.config.Logger = logger
	})
}

func WithUseSSL(useSSL bool) Option {
	return optionFun(func(s *server) {
		s.config.UseSSL = useSSL
	})
}

func WithCertsDir(certsDir string) Option {
	return optionFun(func(s *server) {
		s.config.CertsDir = certsDir
	})
}

func WithSSLCA(sslCA string) Option {
	return optionFun(func(s *server) {
		s.config.SSLCA = sslCA
	})
}

func WithSSLCert(sslCert string) Option {
	return optionFun(func(s *server) {
		s.config.SSLCert = sslCert
	})
}

func WithSSLKey(sslKey string) Option {
	return optionFun(func(s *server) {
		s.config.SSLKey = sslKey
	})
}

func WithRSAKeysDir(rsaKeysDir string) Option {
	return optionFun(func(s *server) {
		s.config.RSAKeysDir = rsaKeysDir
	})
}

func WithCachingSHA2PasswordPrivateKeyPath(privatePath string) Option {
	return optionFun(func(s *server) {
		s.config.CachingSHA2PasswordPrivateKeyPath = privatePath
	})
}

func WithCachingSHA2PasswordPublicKeyPath(publicPath string) Option {
	return optionFun(func(s *server) {
		s.config.CachingSHA2PasswordPublicKeyPath = publicPath
	})
}

func WithSHA256PasswordPrivateKeyPath(privatePath string) Option {
	return optionFun(func(s *server) {
		s.config.SHA256PasswordPrivateKeyPath = privatePath
	})
}

func WithSHA256PasswordPublicKeyPath(publicPath string) Option {
	return optionFun(func(s *server) {
		s.config.SHA256PasswordPublicKeyPath = publicPath
	})
}

type Option interface {
	apply(*server)
}

type optionFun func(*server)

func (f optionFun) apply(s *server) {
	f(s)
}
