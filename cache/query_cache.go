package cache

import (
	"sync"
)

type CachedQuery struct {
	SQL       string
	Args      []any
	ArgsOrder []string
	StmtKey   string
	ScannerID string
}

type QueryCache interface {
	Get(fingerprint uint64) (*CachedQuery, bool)
	Set(fingerprint uint64, sql string, args []any, argsOrder []string, stmtKey string, scannerID string)
}

type memQueryCache struct {
	mu             sync.RWMutex
	data           map[uint64]*CachedQuery
	queryCachePool sync.Pool
}

func NewQueryCache() QueryCache {
	return &memQueryCache{
		data: make(map[uint64]*CachedQuery, 1024),
		queryCachePool: sync.Pool{
			New: func() any {
				return new(CachedQuery)
			},
		},
	}
}

func (c *memQueryCache) Get(f uint64) (*CachedQuery, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	q, ok := c.data[f]
	return q, ok
}

func (c *memQueryCache) Set(f uint64, sql string, args []any, argsOrder []string, stmtKey string, scannerID string) {
	q := c.queryCachePool.Get().(*CachedQuery)
	if q == nil {
		panic("pooled CachedQuery is nil")
	}

	q.SQL = sql
	q.Args = args
	q.ArgsOrder = argsOrder
	q.StmtKey = stmtKey
	q.ScannerID = scannerID

	c.mu.Lock()
	c.data[f] = q
	c.mu.Unlock()
}
