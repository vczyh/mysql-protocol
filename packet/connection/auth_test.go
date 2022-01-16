package connection

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestMySQLNativePassword(t *testing.T) {
	salt1 := []byte{1, 62, 109, 76, 20, 73, 117, 76}
	salt2 := []byte{39, 107, 67, 92, 8, 84, 124, 54, 37, 108, 97, 29, 0}
	salt := make([]byte, len(salt1)+len(salt2)-1)
	copy(salt, salt1)
	copy(salt[len(salt1):], salt2[:len(salt2)-1])


	s := bytes.TrimSpace(salt)
	encryptPassword, err := EncryptPassword(MySQLNativePassword, []byte("Zggyy2019!"), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(encryptPassword)
	t.Log(hex.EncodeToString(encryptPassword))
}
