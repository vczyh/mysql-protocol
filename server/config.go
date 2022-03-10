package server

import (
	"github.com/vczyh/mysql-protocol/auth"
)

type Config struct {
	Port              int
	Version           string
	DefaultAuthMethod auth.Method

	UserProvider UserProvider
	SHA2Cache    SHA2Cache

	CertsDir string

	UseSSL  bool
	SSLCA   string
	SSLCert string
	SSLKey  string

	// private/public key-pair files for sha256_password or caching_sha2_password authentication
	RSAKeysDir string

	SHA256PasswordPrivateKeyPath string
	SHA256PasswordPublicKeyPath  string

	CachingSHA2PasswordPrivateKeyPath string
	CachingSHA2PasswordPublicKeyPath  string

	Handler Handler
	Logger  Logger
}
