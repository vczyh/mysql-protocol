package client

import (
	"mysql-protocol/packet/command"
	"mysql-protocol/packet/generic"
)

type column struct {
	name       string
	length     uint32
	columnType command.TableColumnType
	flags      generic.ColumnDefinitionFlag
	// TODO
}
