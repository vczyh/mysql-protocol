MySQL SDK by Go.

The following has been implemented:

- [Client](#Client)

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

| Implement | Description |
|---- |---- |
| [go-mysql-driver](https://github.com/vczyh/go-mysql-driver) | MySQL driver |

## Server

```go
	srv := NewServer(
		NewTestHandler(),
		WithHost("0.0.0.0"),
		WithPort(3306),
		WithUser("root"),
		WithPassword("root"),
		WithVersion("8.0.25"))

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
```







