package server

import (
	"errors"
	"fmt"
	"github.com/vczyh/mysql-protocol/core"
	"sync"
)

var (
	ErrUserNotFound                      = errors.New("user record not found")
	ErrUserExisted                       = errors.New("user already existed")
	ErrInvalidAuthenticationStringFormat = errors.New("invalid authentication string format")
)

// UserProvider performs Authentication and Authorization.
// Implement should be concurrent safely.
type UserProvider interface {
	// Create will be called to add user if needed when server starts.
	//Create(r *CreateUserRequest) error

	Key(user, host string) (string, error)

	AuthenticationString(key string) ([]byte, error)

	AuthenticationMethod(key string) (core.AuthenticationMethod, error)

	// Authentication checks (user,host) whether login is allowed.
	//
	// Return nil means allowed, If not be allowed, return errors.Error built by
	// errors.AccessDenied(errors.AccessDenied.build()). Also return other errors.
	//Authentication(r *AuthenticationRequest) error

	Authorization(r *AuthorizationRequest) error
}

type AuthorizationRequest struct {
	User   string
	Host   string
	method core.AuthenticationMethod

	// Challenge-Response:
	// It doesn't need know plaintext password, only compares hash values.
	// It's fast.
	//
	// Re-ascertain Password:
	// It needs plaintext password and recalculate authentication_string.
	// It needs some time and is slower than Challenge-Response.
	//
	// len(ChallengeRes)!=0 represents Challenge-Response, otherwise use Re-ascertain Password.
	//
	// mysql_native_password -> SHA1(SHA1(password))
	// sha256_password -> not support Challenge-Response
	// caching_sha2_password -> SHA256(SHA256(password))
	ChallengeRes []byte

	// Password will be used when len(ChallengeRes)==0, using Re-ascertain Password.
	Password string
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
	method               core.AuthenticationMethod
	TLSRequired          bool
}

type CreateUserRequest struct {
	User        string
	Host        string
	Password    string
	Method      core.AuthenticationMethod
	TLSRequired bool

	// expand more params
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
	u := mp.bestMatch(user, host)
	if u == nil {
		return "", ErrUserNotFound
	}
	return mp.userKey(u.Name, u.Host), nil
}

func (mp *memoryUserProvider) AuthenticationString(key string) ([]byte, error) {
	u := mp.getUser(key)
	if u == nil {
		return nil, ErrUserNotFound
	}
	return u.AuthenticationString, nil
}

func (mp *memoryUserProvider) AuthenticationMethod(key string) (core.AuthenticationMethod, error) {
	u := mp.getUser(key)
	if u == nil {
		return core.MySQLNativePassword, ErrUserNotFound
	}
	return u.method, nil
}

func (mp *memoryUserProvider) bestMatch(user, host string) *user {
	// TODO
	if u := mp.getUser(mp.userKey(user, host)); u != nil {
		return u
	}

	if u := mp.getUser(mp.userKey(user, "%")); u != nil {
		return u
	}

	return nil
}

func (mp *memoryUserProvider) Authorization(r *AuthorizationRequest) error {
	//errAccessDenied := errors.AccessDenied.Build(r.User, r.Host, "YES")
	//
	//key := mp.userKey(r.User, r.Host)
	//val, ok := mp.users.Load(key)
	//if !ok {
	//	return errAccessDenied
	//}
	//user := val.(*user)
	//
	//if r.method != user.method {
	//	// TODO plugin mismatch mysql error
	//	return errAccessDenied
	//}
	//
	//
	//err := user.method.Validate(user.AuthenticationString, []byte(r.Password))
	//if err != nil {
	//	if err == mysqlpassword.ErrMismatch {
	//		return errAccessDenied
	//	}
	//	return err
	//}
	//
	//if user.TLSRequired && !r.TLSed {
	//	// TODO should return TLS mysql error
	//	return err
	//}

	return nil
}

//func (mp *memoryUserProvider) challengeResponse(user *user, challengeRes []byte, err error) error {
//	switch user.method {
//	case core.MySQLNativePassword:
//		hexChallengeResStr := strings.ToUpper(hex.EncodeToString(challengeRes))
//		if !bytes.Equal([]byte(hexChallengeResStr), user.AuthenticationString[1:]) {
//			return err
//		}
//		return nil
//
//	case core.CachingSha2Password:
//		val, ok := mp.sha2PasswordCache.Load(mp.userKey(user.Name, user.Host))
//		if ok {
//			if bytes.Equal(val.([]byte), challengeRes) {
//				return nil
//			} else {
//				return errAccessDenied
//			}
//		}
//
//	default:
//		return fmt.Errorf("%s does not support Challenge-Response", user.method)
//	}
//
//}

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
