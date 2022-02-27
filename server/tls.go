package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/vczyh/mysql-protocol/code"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/mysql"
	"github.com/vczyh/mysql-protocol/mysqlerror"
	"github.com/vczyh/mysql-protocol/packet"
	"math/big"
	"os"
	"path"
	"time"
)

var (
	CACertName     = "ca.pem"
	CAKeyName      = "ca-key.pem"
	ClientCertName = "client-cert.pem"
	ClientKeyName  = "client-key.pem"
	ServerCertName = "server-cert.pem"
	ServerKeyName  = "server-key.pem"
)

func (s *server) handleTLSPacket(data []byte, conn mysql.Conn) error {
	pkt, err := packet.ParseSSLRequest(data)
	if err != nil {
		return err
	}

	if pkt.ClientCapabilityFlags&flag.ClientSSL == 0 {
		// ToDO SSL mysql error
		return mysqlerror.NewWithoutSQLState("", code.ErrSendToClient, fmt.Sprintf("%s required", flag.ClientSSL))
	}

	// TODO update capabilities
	conn.SetCapabilities(pkt.ClientCapabilityFlags)
	conn.ServerTLS(s.tlsConfig)

	return nil
}

func (s *server) buildTLSConfig() (err error) {
	if !s.config.UseSSL {
		return nil
	}

	caFile := s.config.SSLCA
	certFile := s.config.SSLCert
	keyFile := s.config.SSLKey

	var cert tls.Certificate
	if certFile == "" || keyFile == "" {
		cert = s.serverCert
	} else {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return err
		}
	}

	var certPool *x509.CertPool
	if caFile == "" {
		certPool = x509.NewCertPool()
		cert := s.caCert.Leaf
		if cert == nil {
			cert, err = x509.ParseCertificate(s.caCert.Certificate[0])
			if err != nil {
				return err
			}
		}
		certPool.AddCert(cert)
	} else {
		caCertBytes, err := os.ReadFile(caFile)
		if err != nil {
			return err
		}
		certPool = x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caCertBytes); !ok {
			// TODO return what?
			return fmt.Errorf("certPool.AppendCertsFromPEM() failed")
		}
	}

	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
	}

	return nil
}

func (s *server) generateReadCerts() error {
	isExist, err := s.isCertsExist()
	if err != nil {
		return err
	}

	if isExist {
		return s.readCerts()
	}

	// First, generate CA private key and cert.
	s.caCert, err = generateCA(s.config.Version + "_Auto_Generated_CA_Certificate")
	if err != nil {
		return err
	}

	// Next, generate Server/Client private key and cert.
	s.serverCert, err = generateCert(s.caCert, s.config.Version+"_Auto_Generated_Server_Certificate")
	if err != nil {
		return err
	}
	s.clientCert, err = generateCert(s.caCert, s.config.Version+"_Auto_Generated_Client_Certificate")
	if err != nil {
		return err
	}

	if s.config.CertsDir == "" {
		return nil
	}

	// Write CA/Server/Client key and cert.
	dir := s.config.CertsDir
	caCertPath := path.Join(dir, CACertName)
	caKeyPath := path.Join(dir, CAKeyName)
	clientCertPath := path.Join(dir, ClientCertName)
	clientKeyPath := path.Join(dir, ClientKeyName)
	serverCertPath := path.Join(dir, ServerCertName)
	serverKeyPath := path.Join(dir, ServerKeyName)

	if err := writeCertPair(s.caCert, caCertPath, caKeyPath); err != nil {
		return err
	}
	if err := writeCertPair(s.serverCert, serverCertPath, serverKeyPath); err != nil {
		return err
	}
	if err := writeCertPair(s.clientCert, clientCertPath, clientKeyPath); err != nil {
		return err
	}

	return nil
}

func (s *server) isCertsExist() (bool, error) {
	dir := s.config.CertsDir
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

	isCACertExist, err := isExistFunc(CACertName)
	if err != nil {
		return false, err
	}
	isCAKeyExist, err := isExistFunc(CAKeyName)
	if err != nil {
		return false, err
	}

	isClientCertExist, err := isExistFunc(ClientCertName)
	if err != nil {
		return false, err
	}
	isClientKeyExist, err := isExistFunc(ClientKeyName)
	if err != nil {
		return false, err
	}

	isServerCertExist, err := isExistFunc(ServerCertName)
	if err != nil {
		return false, err
	}
	isServerKeyExist, err := isExistFunc(ServerKeyName)
	if err != nil {
		return false, err
	}

	if isCACertExist || isCAKeyExist ||
		isClientCertExist || isClientKeyExist ||
		isServerCertExist || isServerKeyExist {
		return true, nil
	}

	return false, nil
}

func (s *server) readCerts() (err error) {
	dir := s.config.CertsDir

	caCertPath := path.Join(dir, CACertName)
	caKeyPath := path.Join(dir, CAKeyName)

	clientCertPath := path.Join(dir, ClientCertName)
	clientKeyPath := path.Join(dir, ClientKeyName)

	serverCertPath := path.Join(dir, ServerCertName)
	serverKeyPath := path.Join(dir, ServerKeyName)

	s.caCert, err = tls.LoadX509KeyPair(caCertPath, caKeyPath)
	if err != nil {
		return err
	}

	s.clientCert, err = tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return err
	}

	s.serverCert, err = tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		return err
	}

	return nil
}

func generateCA(organization string) (tls.Certificate, error) {
	fail := func(err error) (tls.Certificate, error) { return tls.Certificate{}, err }

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fail(err)
	}
	keyUsage := x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign | x509.KeyUsageDataEncipherment

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fail(err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(time.Hour * 24 * 365 * 10)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:    keyUsage,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},

		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDerBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return fail(err)
	}

	keyDerBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fail(err)
	}

	certPEMBlock, err := encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: certDerBytes})
	keyPEMBlock, err := encodePEM(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDerBytes})

	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
}

func generateCert(ca tls.Certificate, organization string) (tls.Certificate, error) {
	fail := func(err error) (tls.Certificate, error) { return tls.Certificate{}, err }

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fail(err)
	}
	keyUsage := x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageDataEncipherment

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fail(err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(time.Hour * 24 * 365 * 10)

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:    keyUsage,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},

		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	caCert := ca.Leaf
	if caCert == nil {
		caCert, err = x509.ParseCertificate(ca.Certificate[0])
		if err != nil {
			return fail(err)
		}
	}
	caKey := ca.PrivateKey

	certDerBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		return fail(err)
	}

	keyDerBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fail(err)
	}

	certPEMBlock, err := encodePEM(&pem.Block{Type: "CERTIFICATE", Bytes: certDerBytes})
	keyPEMBlock, err := encodePEM(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDerBytes})

	return tls.X509KeyPair(certPEMBlock, keyPEMBlock)

}

func writeCertPair(cert tls.Certificate, certFile, keyFile string) error {
	keyDerBytes, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return err
	}
	if err := writePEMFile(true, keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDerBytes}); err != nil {
		return err
	}

	return writePEMFile(false, certFile, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
}
