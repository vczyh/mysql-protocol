package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/packet"
	"os"
)

func (c *conn) handleSSL() error {
	if !c.useSSL {
		return nil
	}

	// TODO server support SSL?

	capabilities := c.Capabilities() | flag.ClientSSL
	c.mysqlConn.SetCapabilities(capabilities)

	if err := c.writeSSLRequestPacket(capabilities); err != nil {
		return err
	}
	return c.switchToTLS()
}

func (c *conn) switchToTLS() error {
	cert, err := tls.LoadX509KeyPair(c.sslCert, c.sslKey)
	if err != nil {
		return fmt.Errorf("load key pair failed: %v", err)
	}

	var certPool *x509.CertPool
	if c.sslCA != "" {
		caCertBytes, err := os.ReadFile(c.sslCA)
		if err != nil {
			return fmt.Errorf("read ca file failed: %v", err)
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caCertBytes); !ok {
			// TODO error
			return fmt.Errorf("certPool.AppendCertsFromPEM()")
		}
	}

	config := &tls.Config{
		ServerName:         c.host,
		InsecureSkipVerify: c.insecureSkipVerify,
		Certificates:       []tls.Certificate{cert},
		RootCAs:            certPool,
	}
	c.mysqlConn.ClientTLS(config)

	return nil
}

func (c *conn) writeSSLRequestPacket(capabilities flag.CapabilityFlag) error {
	return c.WritePacket(&packet.SSLRequest{
		ClientCapabilityFlags: capabilities,
		MaxPacketSize:         maxPacketSize,
		CharacterSet:          c.collation,
	})
}
