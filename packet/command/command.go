package command

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/packet/generic"
	"github.com/vczyh/mysql-protocol/packet/types"
)

func New(cmd generic.Command, data []byte) *generic.Simple {
	data = append([]byte{byte(cmd)}, data...)
	return generic.NewSimple(data)
}

func ParseQueryResponse(data []byte) (uint64, error) {
	if len(data) < 5 {
		return 0, generic.ErrPacketData
	}
	buf := bytes.NewBuffer(data[4:])
	columnCount, err := types.LengthEncodedInteger.Get(buf)
	return columnCount, err
}
