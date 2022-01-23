package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"mysql-protocol/client"
	"net/url"
	"strconv"
)

func init() {
	sql.Register("mysql", &Driver{})
}

type Driver struct {
	host     string
	port     int
	user     string
	password string
	// query values
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	if err := d.parseName(name); err != nil {
		return nil, err
	}

	return createConnection(&config{
		host:     d.host,
		port:     d.port,
		user:     d.user,
		password: d.password,
	})
}

func (d *Driver) parseName(name string) error {
	u, err := url.Parse(name)
	if err != nil {
		return err
	}
	d.host = u.Hostname()
	if d.port, err = strconv.Atoi(u.Port()); err != nil {
		return err
	}
	d.user = u.User.Username()
	if password, set := u.User.Password(); set {
		d.password = password
	}
	// TODO query
	return nil
}

type conn struct {
	config    *config
	mysqlConn *client.Conn
}

type config struct {
	host     string
	port     int
	user     string
	password string
}

func createConnection(config *config) (driver.Conn, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}

	conn := &conn{config: config}
	var err error
	conn.mysqlConn, err = client.CreateConnection(
		client.WithHost(conn.config.host),
		client.WithPort(conn.config.port),
		client.WithUser(conn.config.user),
		client.WithPassword(conn.config.password))

	return conn, err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.mysqlConn.Prepare(query)
}

func (c *conn) Close() error {
	return c.mysqlConn.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	// TODO
	panic("implement me")
}

func (c *conn) Ping(ctx context.Context) error {
	// TODO context
	if err := c.mysqlConn.Ping(); err != nil {
		return driver.ErrBadConn
	}
	return nil
}
