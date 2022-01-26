package driver

import (
	"database/sql"
	"database/sql/driver"
	"net/url"
	"strconv"
	"time"
)

func init() {
	sql.Register("mysql", &Driver{})
}

type Driver struct {
	host     string
	port     int
	user     string
	password string

	// query
	loc *time.Location
}

// Open called by database/sql when need create new mysql connection
// name format: mysql://user:password@ip:port?loc=UTC
func (d *Driver) Open(name string) (driver.Conn, error) {
	if err := d.parseName(name); err != nil {
		return nil, err
	}

	return createConnection(&config{
		host:     d.host,
		port:     d.port,
		user:     d.user,
		password: d.password,
		loc:      d.loc,
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

	query := u.Query()
	if locName := query.Get("loc"); locName != "" {
		if d.loc, err = time.LoadLocation(locName); err != nil {
			return err
		}
	} else {
		d.loc = time.Local
	}

	return nil
}
