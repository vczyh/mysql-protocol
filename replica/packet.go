package replica

import (
	"bytes"
	"github.com/vczyh/mysql-protocol/binlog"
	"github.com/vczyh/mysql-protocol/flag"
	"github.com/vczyh/mysql-protocol/packet"
)

type RegisterReplica struct {
	Command     packet.Command
	ServerId    uint32
	HostnameLen uint8
	Hostname    string
	UserLen     uint8
	User        string
	PasswordLen uint8
	Password    string
	Port        uint16
	Rank        int32 // ignored
	SourceId    uint32
}

func (rr *RegisterReplica) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(rr.Command.Byte())

	// ServerId
	payload.Write(packet.FixedLengthInteger.Dump(uint64(rr.ServerId), 4))

	// Hostname
	payload.Write(packet.FixedLengthInteger.Dump(uint64(len(rr.Hostname)), 1))
	payload.WriteString(rr.Hostname)

	// User
	payload.Write(packet.FixedLengthInteger.Dump(uint64(len(rr.User)), 1))
	payload.WriteString(rr.User)

	// Password
	payload.Write(packet.FixedLengthInteger.Dump(uint64(len(rr.Password)), 1))
	payload.WriteString(rr.Password)

	// Port
	payload.Write(packet.FixedLengthInteger.Dump(uint64(rr.Port), 2))

	// Rank
	payload.Write(bytes.Repeat([]byte{0x00}, 4))

	// Source Id
	payload.Write(packet.FixedLengthInteger.Dump(uint64(rr.SourceId), 4))

	return payload.Bytes(), nil
}

type BinlogDump struct {
	Command  packet.Command
	Position uint32
	Flags    binlog.DumpFlag
	ServerId uint32
	FileName string
}

func (b *BinlogDump) Dump(capabilities flag.Capability) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(b.Command.Byte())

	// DumpFlag file position
	payload.Write(packet.FixedLengthInteger.Dump(uint64(b.Position), 4))

	// Flags
	payload.Write(packet.FixedLengthInteger.Dump(uint64(b.Flags), 2))

	// Replica server id
	payload.Write(packet.FixedLengthInteger.Dump(uint64(b.ServerId), 4))

	// DumpFlag file name
	payload.WriteString(b.FileName)

	return payload.Bytes(), nil
}
