package replica

import (
	"crypto/rand"
	"fmt"
	"github.com/vczyh/mysql-protocol/binlog"
	"github.com/vczyh/mysql-protocol/client"
	"github.com/vczyh/mysql-protocol/packet"
	"math/big"
)

type Replica struct {
	host     string
	port     int
	user     string
	password string
	serverId uint32

	conn *client.Conn
}

func NewReplica(opts ...Option) *Replica {
	r := new(Replica)
	for _, opt := range opts {
		opt.apply(r)
	}
	return r
}

func (r *Replica) StartDump(file string, position int) (*Streamer, error) {
	if err := r.build(); err != nil {
		return nil, err
	}

	if _, err := r.conn.Exec("SET @master_binlog_checksum= @@global.binlog_checksum"); err != nil {
		return nil, err
	}

	if err := r.writeRegisterReplicaPacket(); err != nil {
		return nil, err
	}
	if err := r.readOKERRPacket(); err != nil {
		return nil, err
	}

	if err := r.writeBinlogDumpPacket(file, uint32(position)); err != nil {
		return nil, err
	}

	return r.stream(), nil
}

func (r *Replica) StartDumpGTID() error {
	// TODO implement
	return nil
}

func (r *Replica) stream() *Streamer {
	s := new(Streamer)
	s.c = make(chan *eventDesc)

	go func() {
		defer close(s.c)
		parser := binlog.NewParser()
		for {
			data, err := r.conn.ReadPacket()
			if err != nil {
				s.err = err
				return
			}

			switch {
			case packet.IsErr(data):
				errPkt, err := packet.ParseERR(data, r.conn.Capabilities())
				if err != nil {
					s.err = err
					return
				}
				s.err = errPkt
				return
			case packet.IsEOF(data):
				return
			default:
				e, err := parser.ParseEvent(data[1:])
				s.c <- &eventDesc{
					event: e,
					err:   err,
				}
				if err != nil {
					return
				}
			}
		}
	}()

	return s
}

func (r *Replica) build() (err error) {
	if r.serverId == 0 {
		bigN, err := rand.Int(rand.Reader, big.NewInt(2<<32))
		if err != nil {
			return err
		}
		r.serverId = uint32(bigN.Uint64())
	}

	r.conn, err = client.CreateConnection(
		client.WithHost(r.host),
		client.WithPort(r.port),
		client.WithUser(r.user),
		client.WithPassword(r.password))

	return err
}

func (r *Replica) readOKERRPacket() error {
	data, err := r.conn.ReadPacket()
	if err != nil {
		return err
	}

	switch {
	case packet.IsOK(data):
		return nil
	case packet.IsErr(data):
		pktERR, err := packet.ParseERR(data, r.conn.Capabilities())
		if err != nil {
			return err
		}
		return pktERR
	default:
		return fmt.Errorf("data is not either ok or error packet")
	}
}

func (r *Replica) writeRegisterReplicaPacket() error {
	return r.conn.WriteCommandPacket(&RegisterReplica{
		Command:  packet.ComRegisterSlave,
		ServerId: r.serverId,
	})
}

func (r *Replica) writeBinlogDumpPacket(file string, position uint32) error {
	return r.conn.WriteCommandPacket(&BinlogDump{
		Command:  packet.ComBinlogDump,
		Position: position,
		Flags:    binlog.DumpFlagThroughPosition,
		ServerId: r.serverId,
		FileName: file,
	})
}

func WithHost(host string) Option {
	return optionFunc(func(r *Replica) {
		r.host = host
	})
}

func WithPort(port int) Option {
	return optionFunc(func(r *Replica) {
		r.port = port
	})
}

func WithUser(user string) Option {
	return optionFunc(func(r *Replica) {
		r.user = user
	})
}
func WithPassword(password string) Option {
	return optionFunc(func(r *Replica) {
		r.password = password
	})
}

func WithServerId(serverId uint32) Option {
	return optionFunc(func(r *Replica) {
		r.serverId = serverId
	})
}

type Option interface {
	apply(*Replica)
}

type optionFunc func(*Replica)

func (f optionFunc) apply(r *Replica) {
	f(r)
}
