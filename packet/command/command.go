package command

import (
	"github.com/vczyh/mysql-protocol/packet/generic"
)

func New(cmd generic.Command, data []byte) *generic.Simple {
	data = append([]byte{byte(cmd)}, data...)
	return generic.NewSimple(data)
}
