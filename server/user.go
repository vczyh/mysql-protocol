package server

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/auth"
	"sync"
)

var (
	ErrAccessDenied                      = errors.New("server: matching user not found")
	ErrUserExisted                       = errors.New("server: user existed")
	ErrInvalidAuthenticationStringFormat = errors.New("server: invalid authentication string format")
)

// UserProvider performs Authentication and Authorization.
// Implement should keep concurrent safely.
type UserProvider interface {
	// Key return unique key that represents one matched query record.
	//
	// It should return ErrAccessDenied when the record is not found.
	Key(user, host string) (string, error)

	// AuthenticationMethod return authentication plugin.
	// It equals plugin column of mysql.user table.
	//
	// It should return ErrAccessDenied when record to which the key points is not found.
	AuthenticationMethod(key string) (auth.Method, error)

	// AuthenticationString return hashing string generated by plugin.
	// It equals authentication_string column of mysql.user table.
	//
	// It should return ErrAccessDenied when record to which the key points is not found.
	AuthenticationString(key string) ([]byte, error)

	// Authorization verifies access control.
	// Including, but are not limited to TLS, Database.
	//
	// It should return ErrAccessDenied when record to which the key points is not found,
	// otherwise return mysqlerror.Error depending on the situation if authorization not pass.
	Authorization(key string, r *AuthorizationRequest) error
}

type AuthorizationRequest struct {
	Database string
	TLSed    bool

	// expand more params
}

type memoryUserProvider struct {
	users sync.Map
}

type user struct {
	Name                 string
	Host                 string
	AuthenticationString []byte
	method               auth.Method
	TLSRequired          bool
}

type CreateUserRequest struct {
	User        string
	Host        string
	Password    string
	Method      auth.Method
	TLSRequired bool
}

func NewMemoryUserProvider() *memoryUserProvider {
	return &memoryUserProvider{}
}

func (mp *memoryUserProvider) Create(r *CreateUserRequest) error {
	key := mp.userKey(r.User, r.Host)
	if _, ok := mp.users.Load(key); ok {
		return ErrUserExisted
	}

	user := &user{
		Name:        r.User,
		Host:        r.Host,
		method:      r.Method,
		TLSRequired: r.TLSRequired,
	}

	authenticationString, err := user.method.GenerateAuthenticationStringWithoutSalt([]byte(r.Password))
	if err != nil {
		return err
	}
	user.AuthenticationString = authenticationString

	mp.users.Store(key, user)
	return nil
}

func (mp *memoryUserProvider) Key(user, host string) (string, error) {
	u := mp.simpleBestMatch(user, host)
	if u == nil {
		return "", ErrAccessDenied
	}
	return mp.userKey(u.Name, u.Host), nil
}

func (mp *memoryUserProvider) AuthenticationString(key string) ([]byte, error) {
	u := mp.getUser(key)
	if u == nil {
		return nil, ErrAccessDenied
	}
	return u.AuthenticationString, nil
}

func (mp *memoryUserProvider) AuthenticationMethod(key string) (auth.Method, error) {
	u := mp.getUser(key)
	if u == nil {
		return auth.MySQLNativePassword, ErrAccessDenied
	}
	return u.method, nil
}

func (mp *memoryUserProvider) Authorization(key string, r *AuthorizationRequest) error {
	u := mp.getUser(key)
	if u == nil {
		return ErrAccessDenied
	}

	if u.TLSRequired && !r.TLSed {
		return ErrAccessDenied
	}
	return nil
}

func (mp *memoryUserProvider) userKey(user, host string) string {
	return fmt.Sprintf("%s@%s", user, host)
}

func (mp *memoryUserProvider) getUser(key string) *user {
	val, ok := mp.users.Load(key)
	if !ok {
		return nil
	}
	return val.(*user)
}

func (mp *memoryUserProvider) simpleBestMatch(user, host string) *user {
	if u := mp.getUser(mp.userKey(user, host)); u != nil {
		return u
	}

	if u := mp.getUser(mp.userKey(user, "%")); u != nil {
		return u
	}
	return nil
}
