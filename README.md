MySQL SDK by Go.

The following has been implemented:

- [Client](#Client)
- [Server](#Server)

## Install

```shell
go get github.com/vczyh/mysql-protocol
```

## Client

```go
conn, err := client.CreateConnection(
    client.WithHost("10.0.44.59"),
    client.WithPort(3306),
    client.WithUser("root"),
    client.WithPassword("Unicloud@1221"))

if err != nil {
	// handle error
}

if err := conn.Ping(); err != nil {
	// handle error
}
```

## Server

```go
userProvider := server.NewMemoryUserProvider()
// user1
_ = userProvider.Create(&server.CreateUserRequest{
  User:        "root",
  Host:        "%",
  Password:    "123456",
  Method:      auth.SHA256Password,
  TLSRequired: false,
})
// user2
_ = userProvider.Create(&server.CreateUserRequest{
  User:        "root2",
  Host:        "%",
  Password:    "123456",
  Method:      auth.CachingSha2Password,
  TLSRequired: false,
})

srv := server.NewServer(
  userProvider,
  server.NewDefaultHandler(),
  server.WithPort(3306),
)

_ = srv.Start()
```

### Flags

| name                        | default               | description        |
| --------------------------- | --------------------- | ------------------ |
| **`WithVersion()`**         | ""                    | Version identifier. |
| **`WithDefaultAuthMethod()`** | `mysql_native_password` | Authentication plugin. |
| **`WithSHA2Cache()`** | `DefaultSHA2Cache` | `caching_sha2_password` caching function implement. |
| **`WithLogger()`** | `DefaultLogger` | Implement of logger write all messages to. |
| **`WithUseSSL()`** | `false` | Whether to open SSL/TLS. Use automatically generated key and certificates if it's true and `WithSSLCA()` `WithSSLCert()` `WithSSLKey()`are not specified. |
| **`WithCertsDir()`** | "" | At startup, the server automatically generates server-side and client-side SSL/TLS certificate and key files, include CA certificate and key file. Default don't write them to local file system.  If `WithCertsDir()` not empty, write those files to the directory, otherwise read them instead of generating. |
| **`WithSSLCA()`** | automatically generate | The path name of the Certificate Authority (CA) certificate file in PEM format. The file contains a list of trusted SSL Certificate Authorities. |
| **`WithSSLCert()`** | automatically generate | The path name of the server SSL public key certificate file in PEM format. |
| **`WithSSLKey()`** | automatically generate | The path name of the server SSL private key file in PEM format. |
| **`WithRSAKeysDir()`** | "" | At startup, the server automatically generates private and public key. Default don't write them to local file system.  If `WithRSAKeysDir()` not empty, write those files to the directory, otherwise read them instead of generating. The key pair is used by `sha256_password` when `WithSHA256PasswordPrivateKeyPath()` `WithSHA256PasswordPublicKeyPath()` are not specified, or used by `caching_sha2_password` when `WithCachingSHA2PasswordPrivateKeyPath()` `WithCachingSHA2PasswordPublicKeyPath()`are not specified. |
| **`WithCachingSHA2PasswordPrivateKeyPath()`** | automatically generate |  |
| **`WithCachingSHA2PasswordPublicKeyPath()`** | automatically generate |  |
| **`WithSHA256PasswordPrivateKeyPath()`** | automatically generate |  |
| **`WithSHA256PasswordPublicKeyPath()`** | automatically generate |  |





