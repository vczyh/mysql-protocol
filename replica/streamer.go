package replica

import "github.com/vczyh/mysql-protocol/binlog"

type eventDesc struct {
	event binlog.Event
	err   error
}

type Streamer struct {
	c   chan *eventDesc
	e   binlog.Event
	err error
}

func (s *Streamer) HasNext() bool {
	if s.err != nil {
		return false
	}

	e, ok := <-s.c
	if !ok {
		return false
	}
	if e.err != nil {
		s.err = e.err
		return false
	}

	s.e = e.event
	return true
}

func (s *Streamer) Next() binlog.Event {
	return s.e
}

func (s *Streamer) Err() error {
	return s.err
}
