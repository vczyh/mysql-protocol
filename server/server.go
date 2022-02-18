package server

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet"
	"log"
	"math/big"
	"net"
)

type server struct {
	h Handler

	host     string
	port     int
	user     string
	password string
	version  string
	plugin   core.AuthenticationPlugin

	useSSL    bool
	sslCA     string
	sslCert   string
	sslKey    string
	tlsConfig *tls.Config

	l net.Listener
}

func NewServer(h Handler, opts ...Option) *server {
	s := new(server)
	s.h = h

	for _, opt := range opts {
		opt.apply(s)
	}
	return s
}

func (s *server) Start() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		return err
	}
	defer l.Close()
	s.l = l

	if err := s.buildTLSConfig(); err != nil {
		return err
	}

	for {
		conn, err := s.l.Accept()
		if err != nil {
			// TODO log error
			log.Printf("accept error: %v", err)
			continue
		}

		connId, err := s.applyForConnectionId()
		if err != nil {
			// TODO log error
			log.Printf("create mysql connection error: %v", err)
			continue
		}

		go s.handleConnection(mysql.NewServerConnection(conn, connId, s.defaultCapabilities()))
	}
}

func (s *server) handleConnection(conn mysql.Conn) {
	defer s.closeConnection(conn)

	if err := s.auth(conn); err != nil {
		// TOOD log
		log.Printf("auth error: %v", err)
		return
	}

	for {
		if conn.Closed() {
			return
		}
		if err := s.handleCommand(conn); err != nil {
			// TODO log
			log.Printf("can't handle command error: %v, so close connection", err)
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

func (s *server) defaultCapabilities() core.CapabilityFlag {
	capabilities := core.ClientLongPassword |
		core.ClientFoundRows |
		core.ClientLongFlag |
		core.ClientConnectWithDB |
		core.ClientNoSchema |
		core.ClientCompress |
		core.ClientODBC |
		core.ClientLocalFiles |
		core.ClientIgnoreSpace |
		core.ClientProtocol41 |
		core.ClientInteractive |
		core.ClientIgnoreSigpipe |
		core.ClientTransactions |
		core.ClientSecureConnection |
		core.ClientMultiStatements |
		core.ClientMultiResults |
		core.ClientPsMultiResults |
		core.ClientPluginAuth |
		core.ClientConnectAttrs |
		core.ClientPluginAuthLenencClientData |
		core.ClientCanHandleExpiredPasswords
	//generic.ClientSessionTrack |
	//generic.ClientDeprecateEOF

	if s.useSSL {
		capabilities |= core.ClientSSL
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

func WithHost(host string) Option {
	return optionFun(func(s *server) {
		s.host = host
	})
}

func WithPort(port int) Option {
	return optionFun(func(s *server) {
		s.port = port
	})
}

func WithUser(user string) Option {
	return optionFun(func(s *server) {
		s.user = user
	})
}

func WithPassword(password string) Option {
	return optionFun(func(s *server) {
		s.password = password
	})
}

func WithVersion(version string) Option {
	return optionFun(func(s *server) {
		s.version = version
	})
}

func WithAuthPlugin(plugin core.AuthenticationPlugin) Option {
	return optionFun(func(s *server) {
		s.plugin = plugin
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

type Option interface {
	apply(*server)
}

type optionFun func(*server)

func (f optionFun) apply(s *server) {
	f(s)
}
