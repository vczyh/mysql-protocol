package server

import "sync"

type SHA2Cache interface {
	Put(key string, val []byte)

	Get(key string) []byte

	Delete(key string)

	Clear()
}

type DefaultSHA2Cache struct {
	m sync.Map
}

func NewDefaultSHA2Cache() SHA2Cache {
	return &DefaultSHA2Cache{}
}

func (c *DefaultSHA2Cache) Put(key string, val []byte) {
	c.m.Store(key, val)
}

func (c *DefaultSHA2Cache) Get(key string) []byte {
	val, ok := c.m.Load(key)
	if ok {
		return val.([]byte)
	}
	return nil
}

func (c *DefaultSHA2Cache) Delete(key string) {
	c.m.Delete(key)
}

func (c *DefaultSHA2Cache) Clear() {
	c.m.Range(func(key, value interface{}) bool {
		c.m.Delete(key)
		return true
	})
}
