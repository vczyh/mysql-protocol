package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/mysqlerror"
	"github.com/vczyh/mysql-protocol/packet"
	"os"
)

func (s *server) handleTLS(data []byte, conn mysql.Conn) error {
	pkt, err := packet.ParseSSLRequest(data)
	if err != nil {
		return err
	}

	if pkt.ClientCapabilityFlags&flag.ClientSSL == 0 {
		// ToDO SSL mysql error
		return mysqlerror.NewWithoutSQLState("", code.ErrGeneral, fmt.Sprintf("%s required", flag.ClientSSL))
	}

	// TODO update capabilities
	conn.SetCapabilities(pkt.ClientCapabilityFlags)
	conn.ServerTLS(s.tlsConfig)

	return nil
}

func (s *server) buildTLSConfig() error {
	if !s.useSSL {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(s.sslCert, s.sslKey)
	if err != nil {
		return fmt.Errorf("load key pair failed: %v", err)
	}

	var certPool *x509.CertPool
	if s.sslCA != "" {
		caCertBytes, err := os.ReadFile(s.sslCA)
		if err != nil {
			return fmt.Errorf("read ca file failed: %v", err)
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caCertBytes); !ok {
			// TODO error
			return fmt.Errorf("certPool.AppendCertsFromPEM()")
		}
	}

	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
	}

	return nil
}
