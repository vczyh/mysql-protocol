package auth

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	mysqlpassword "github.com/vczyh/mysql-password"
	"github.com/vczyh/mysql-protocol/rand"
	"strconv"
)

var (
	ErrUnsupportedAuthenticationMethod = errors.New("auth: unsupported AuthenticationMethod")
	ErrMismatch                        = errors.New("auth: validate mismatch")
)

type AuthenticationMethod uint8

const (
	MySQLNativePassword AuthenticationMethod = iota
	SHA256Password
	CachingSha2Password
)

func ParseAuthenticationPlugin(name string) (AuthenticationMethod, error) {
	switch name {
	case MySQLNativePassword.String():
		return MySQLNativePassword, nil
	case SHA256Password.String():
		return SHA256Password, nil
	case CachingSha2Password.String():
		return CachingSha2Password, nil
	default:
		return MySQLNativePassword, fmt.Errorf("unknown auth method: %v", name)
	}
}

func (m AuthenticationMethod) GenerateAuthenticationString(password, salt []byte) ([]byte, error) {
	switch m {
	case MySQLNativePassword:
		return mysqlpassword.NewMySQLNative().Encrypt(password, salt)
	case SHA256Password:
		return mysqlpassword.NewSHA256().Encrypt(password, salt)
	case CachingSha2Password:
		return mysqlpassword.NewCachingSHA2().Encrypt(password, salt)
	default:
		return nil, ErrUnsupportedAuthenticationMethod
	}
}

func (m AuthenticationMethod) GenerateAuthenticationStringWithoutSalt(password []byte) ([]byte, error) {
	var salt []byte
	switch m {
	case MySQLNativePassword:
		salt = nil
	case SHA256Password:
		salt = rand.Bytes(20)
	case CachingSha2Password:
		salt = make([]byte, 27)
		copy(salt, "$A$")

		val := strconv.FormatInt(int64(mysqlpassword.RoundsDefault/1000), 16)
		for i := len(val); i < 3; i++ {
			val = "0" + val
		}
		copy(salt[3:], val)
		copy(salt[3+3:], "$")
		copy(salt[3+3+1:], rand.Bytes(20))
	}

	return m.GenerateAuthenticationString(password, salt)
}

func (m AuthenticationMethod) Validate(authenticationStr, password []byte) error {
	var err error
	switch m {
	case MySQLNativePassword:
		err = mysqlpassword.NewMySQLNative().Validate(authenticationStr, password)
	case SHA256Password:
		err = mysqlpassword.NewSHA256().Validate(authenticationStr, password)
	case CachingSha2Password:
		err = mysqlpassword.NewCachingSHA2().Validate(authenticationStr, password)
	default:
		return ErrUnsupportedAuthenticationMethod
	}

	if err == mysqlpassword.ErrMismatch {
		return ErrMismatch
	}
	return err
}

func (m AuthenticationMethod) EncryptPassword(password, salt []byte) ([]byte, error) {
	switch m {
	// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
	case MySQLNativePassword:
		h := sha1.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		mix := make([]byte, len(salt)+len(stage2))
		copy(mix, salt)
		copy(mix[len(salt):], stage2)
		h.Reset()
		h.Write(mix)
		stage3 := h.Sum(nil)

		stage4 := make([]byte, 20)
		for i := 0; i < 20; i++ {
			stage4[i] = stage1[i] ^ stage3[i]
		}
		return stage4, nil

	// XOR(SHA256(PASSWORD), SHA256(SHA256(SHA256(PASSWORD)), seed_bytes))
	case CachingSha2Password:
		h := sha256.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		h.Reset()
		h.Write(stage2)
		h.Write(salt)
		stage3 := h.Sum(nil)

		for i := range stage1 {
			stage1[i] ^= stage3[i]
		}
		return stage1, nil

	default:
		return nil, ErrUnsupportedAuthenticationMethod
	}
}

func (m AuthenticationMethod) GenerateChallengeData(password []byte) ([]byte, error) {
	switch m {
	case MySQLNativePassword:
		h := sha1.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)
		return stage2, nil

	case CachingSha2Password:
		h := sha256.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)
		return stage2, nil

	default:
		return nil, ErrUnsupportedAuthenticationMethod
	}
}

// ChallengeResponse process authentication.
// It doesn't need know plaintext password, only compares hash values.
// It's fast.
//
// challengeRes param:
//	mysql_native_password -> SHA1(SHA1(password))
// 	sha256_password -> not support ChallengeResponse
// 	caching_sha2_password -> SHA256(SHA256(password))
func (m AuthenticationMethod) ChallengeResponse(challengeRes, authRes, salt []byte) error {
	switch m {
	case MySQLNativePassword:
		h := sha1.New()

		h.Write(salt)
		h.Write(challengeRes)
		stage1 := h.Sum(nil)

		for i := range stage1 {
			stage1[i] ^= authRes[i]
		}

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		if !bytes.Equal(stage2, challengeRes) {
			return ErrMismatch
		}
		return nil

	case CachingSha2Password:
		h := sha256.New()

		h.Write(challengeRes)
		h.Write(salt)
		stage1 := h.Sum(nil)

		for i := range stage1 {
			stage1[i] ^= authRes[i]
		}

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		if !bytes.Equal(stage2, challengeRes) {
			return ErrMismatch
		}
		return nil

	default:
		return ErrUnsupportedAuthenticationMethod
	}
}

// ReAscertainPassword process authentication.
// It needs plaintext password and recalculate authentication_string.
// It needs some time and is slower than ChallengeResponse.
func (m AuthenticationMethod) ReAscertainPassword() {
	//switch m {
	//case MySQLNativePassword:
	//	hexChallengeResStr := strings.ToUpper(hex.EncodeToString(challengeRes))
	//	if !bytes.Equal([]byte(hexChallengeResStr), user.AuthenticationString[1:]) {
	//		return err
	//	}
	//	return nil
	//}
}

func (m AuthenticationMethod) String() string {
	switch m {
	case MySQLNativePassword:
		return "mysql_native_password"
	case SHA256Password:
		return "sha256_password"
	case CachingSha2Password:
		return "caching_sha2_password"
	default:
		return ErrUnsupportedAuthenticationMethod.Error()
	}
}

// TODO
func EncryptPassword(plugin AuthenticationMethod, password, salt []byte) ([]byte, error) {
	switch plugin {
	// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
	case MySQLNativePassword:
		h := sha1.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		mix := make([]byte, len(salt)+len(stage2))
		copy(mix, salt)
		copy(mix[len(salt):], stage2)
		h.Reset()
		h.Write(mix)
		stage3 := h.Sum(nil)

		stage4 := make([]byte, 20)
		for i := 0; i < 20; i++ {
			stage4[i] = stage1[i] ^ stage3[i]
		}
		return stage4, nil

	// XOR(SHA256(PASSWORD), SHA256(SHA256(SHA256(PASSWORD)), seed_bytes))
	case CachingSha2Password:
		h := sha256.New()
		h.Write(password)
		stage1 := h.Sum(nil)

		h.Reset()
		h.Write(stage1)
		stage2 := h.Sum(nil)

		h.Reset()
		h.Write(stage2)
		h.Write(salt)
		stage3 := h.Sum(nil)

		for i := range stage1 {
			stage1[i] ^= stage3[i]
		}
		return stage1, nil

	default:
		return nil, ErrUnsupportedAuthenticationMethod
	}
}
