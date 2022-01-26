package generic

import (
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
)

type AuthenticationPlugin uint8

const (
	MySQLNativePasswordPlugin AuthenticationPlugin = iota
	CachingSHA2PasswordPlugin
)

func ParseAuthenticationPlugin(name string) (AuthenticationPlugin, error) {
	switch name {
	case MySQLNativePasswordPlugin.String():
		return MySQLNativePasswordPlugin, nil
	case CachingSHA2PasswordPlugin.String():
		return CachingSHA2PasswordPlugin, nil
	default:
		return MySQLNativePasswordPlugin, fmt.Errorf("unknown auth plugin")
	}
}

func (p AuthenticationPlugin) String() string {
	switch p {
	case MySQLNativePasswordPlugin:
		return "mysql_native_password"
	case CachingSHA2PasswordPlugin:
		return "caching_sha2_password"
	default:
		return "Unknown AuthenticationPlugin"
	}
}

func EncryptPassword(plugin AuthenticationPlugin, password, salt []byte) ([]byte, error) {
	switch plugin {
	// https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
	case MySQLNativePasswordPlugin:
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
	case CachingSHA2PasswordPlugin:
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
		return nil, fmt.Errorf("unsupported authentication plugin")
	}
}
