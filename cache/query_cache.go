package cache

import (
	"sync"
)

type CachedQuery struct {
	SQL       string
	ArgsOrder []string
	StmtKey   string
	ScannerID string
}

type QueryCache interface {
	GetSQL(fingerprint uint64) (*CachedQuery, bool)
	SetSQL(fingerprint uint64, q *CachedQuery)
}

type memQueryCache struct {
	mu   sync.RWMutex
	data map[uint64]*CachedQuery
}

func NewQueryCache() QueryCache {
	return &memQueryCache{
		data: make(map[uint64]*CachedQuery, 1024),
	}
}

func (c *memQueryCache) GetSQL(f uint64) (*CachedQuery, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	q, ok := c.data[f]
	return q, ok
}

func (c *memQueryCache) SetSQL(f uint64, q *CachedQuery) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[f] = q
}
