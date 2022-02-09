package server

import (
	"fmt"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"log"
	"net"
)

type server struct {
	host     string
	port     int
	user     string
	password string
	h        Handler

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
	s.l = l

	for {
		conn, err := s.l.Accept()
		if err != nil {
			// TODO log error
			log.Printf("accept error: %v", err)
			continue
		}

		mysqlConn, err := mysql.NewConnection(conn, s.defaultCapabilities())
		if err != nil {
			// TODO log error
			log.Printf("create mysql connection error: %v", err)
			continue
		}
		go s.handleConnection(mysqlConn)
	}
}

func (s *server) handleConnection(conn mysql.Conn) {
	defer conn.Close()

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
			log.Printf("can't handle error: %v, so close connection", err)
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
	case generic.IsPing(data):
		mysqlErr := s.h.Ping()
		if mysqlErr == nil {
			err = conn.WriteEmptyOK()
		} else {
			err = conn.WritePacket(mysqlErr.Packet())
		}

	case generic.IsQuery(data):
		rs, mysqlErr := s.h.Query(string(data[5:]))
		if mysqlErr != nil {
			err = conn.WritePacket(mysqlErr.Packet())
			break
		}
		err = rs.WriteText(conn)

	case generic.IsQuit(data):
		conn.Close()
		s.h.Quit()

	default:
		s.h.Other(data, conn)
	}

	return err
}

func (s *server) defaultCapabilities() generic.CapabilityFlag {
	capabilities := generic.ClientLongPassword |
		generic.ClientFoundRows |
		generic.ClientLongFlag |
		generic.ClientConnectWithDB |
		generic.ClientNoSchema |
		generic.ClientCompress |
		generic.ClientODBC |
		generic.ClientLocalFiles |
		generic.ClientIgnoreSpace |
		generic.ClientProtocol41 |
		generic.ClientInteractive |
		//generic.ClientSSL |
		generic.ClientIgnoreSigpipe |
		generic.ClientTransactions |
		generic.ClientSecureConnection |
		generic.ClientMultiStatements |
		generic.ClientMultiResults |
		generic.ClientPsMultiResults |
		generic.ClientPluginAuth |
		generic.ClientConnectAttrs |
		generic.ClientPluginAuthLenencClientData |
		generic.ClientCanHandleExpiredPasswords
	//generic.ClientSessionTrack |
	//generic.ClientDeprecateEOF

	return capabilities
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

type Option interface {
	apply(*server)
}

type optionFun func(*server)

func (f optionFun) apply(s *server) {
	f(s)
}
