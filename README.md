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
srv := server.NewServer(
  server.NewDefaultHandler(),
  server.WithHost("0.0.0.0"),
  server.WithPort(3306),
  server.WithUser("root"),
  server.WithPassword("root"))

if err := srv.Start(); err != nil {
  // handle error
}
```







