package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/mysqllog"
	"github.com/vczyh/mysql-protocol/packet"
	"math/big"
	"net"
	"os"
)

type server struct {
	port              int
	version           string
	defaultAuthMethod auth.AuthenticationMethod

	userProvider UserProvider
	sha2Cache    SHA2Cache

	useSSL    bool
	sslCA     string
	sslCert   string
	sslKey    string
	tlsConfig *tls.Config

	// private/public key-pair files for sha256_password or caching_sha2_password authentication
	privatePath    string
	publicPath     string
	privateKey     *rsa.PrivateKey
	publicKeyBytes []byte

	h      Handler
	logger mysqllog.Logger

	l net.Listener
}

func NewServer(userProvider UserProvider, h Handler, opts ...Option) *server {
	s := new(server)
	s.userProvider = userProvider
	s.h = h

	for _, opt := range opts {
		opt.apply(s)
	}

	return s
}

func (s *server) Start() error {
	if err := s.build(); err != nil {
		return err
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	defer l.Close()
	s.l = l

	for {
		conn, err := s.l.Accept()
		if err != nil {
			s.logger.Error(fmt.Errorf("tcp accept failed: %v", err))
			continue
		}

		connId, err := s.applyForConnectionId()
		if err != nil {
			s.logger.Error(fmt.Errorf("apply for connection id failed: %v", err))
			continue
		}
		go s.handleConnection(mysql.NewServerConnection(conn, connId, s.defaultCapabilities()))
	}
}

func (s *server) build() error {
	if s.sha2Cache == nil {
		s.sha2Cache = NewDefaultSHA2Cache()
	}

	if s.userProvider == nil {
		return fmt.Errorf("require UserProvider not nil")
	}

	if s.h == nil {
		return fmt.Errorf("require Handler not nil")
	}

	if s.logger == nil {
		s.logger = mysqllog.NewDefaultLogger(mysqllog.SystemLevel, os.Stdout)
	}

	if err := s.buildKeyPair(); err != nil {
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
		if err := conn.WriteError(err); err != nil {
			s.logger.Error(fmt.Errorf("write error packet failed: %v", err))
		}
		return
	}

	for {
		if conn.Closed() {
			return
		}
		if err := s.handleCommand(conn); err != nil {
			s.logger.Error(fmt.Errorf("can't handle command error: %v, so close the connection", err))
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
		err := s.h.Ping()
		if err == nil {
			err = conn.WriteEmptyOK()
		} else {
			err = conn.WriteError(err)
		}

	case packet.IsQuery(data):
		rs, err := s.h.Query(string(data[5:]))
		if err != nil {
			err = conn.WriteError(err)
			break
		}
		err = rs.WriteText(conn)

	case packet.IsQuit(data):
		s.closeConnection(conn)
		s.h.Quit()

	default:
		s.h.Other(data, conn)
	}

	return err
}

func (s *server) defaultCapabilities() flag.CapabilityFlag {
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

	if s.useSSL {
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
	s.h.OnClose(conn.ConnectionId())
}

func (s *server) clientHost(conn mysql.Conn) string {
	addr := conn.RemoteAddr()
	switch v := addr.(type) {
	case *net.TCPAddr:
		return v.IP.String()
	default:
		return ""
	}
}

func WithPort(port int) Option {
	return optionFun(func(s *server) {
		s.port = port
	})
}

func WithVersion(version string) Option {
	return optionFun(func(s *server) {
		s.version = version
	})
}

func WithDefaultAuthMethod(method auth.AuthenticationMethod) Option {
	return optionFun(func(s *server) {
		s.defaultAuthMethod = method
	})
}

func WithUserProvider(userProvider UserProvider) Option {
	return optionFun(func(s *server) {
		s.userProvider = userProvider
	})
}

func WithSHA2Cache(cache SHA2Cache) Option {
	return optionFun(func(s *server) {
		s.sha2Cache = cache
	})
}

func WithLogger(logger mysqllog.Logger) Option {
	return optionFun(func(s *server) {
		s.logger = logger
	})
}

func WithUseSSL(useSSL bool) Option {
	return optionFun(func(s *server) {
		s.useSSL = useSSL
	})
}

func WithSSLCA(sslCA string) Option {
	return optionFun(func(s *server) {
		s.sslCA = sslCA
	})
}

func WithSSLCert(sslCert string) Option {
	return optionFun(func(s *server) {
		s.sslCert = sslCert
	})
}

func WithSSLKey(sslKey string) Option {
	return optionFun(func(s *server) {
		s.sslKey = sslKey
	})
}

//func WithCachingSHA2PasswordAutoGenerateRSAKeys(private string) Option {
//	return optionFun(func(s *server) {
//		s.sslKey = sslKey
//	})
//}

func WithCachingSHA2PasswordPrivateKeyPath(privatePath string) Option {
	return optionFun(func(s *server) {
		s.privatePath = privatePath
	})
}

func WithCachingSHA2PasswordPublicKeyPath(publicPath string) Option {
	return optionFun(func(s *server) {
		s.publicPath = publicPath
	})
}

type Option interface {
	apply(*server)
}

type optionFun func(*server)

func (f optionFun) apply(s *server) {
	f(s)
}
