package cache

import (
	"database/sql"
	"sync"
)

type StatementCache interface {
	Prepare(query string) (*sql.Stmt, error)
	Close() error
}

type memStatementCache struct {
	mu    sync.RWMutex
	stmts map[string]*sql.Stmt
	db    *sql.DB
}

func NewStatementCache(db *sql.DB) StatementCache {
	return &memStatementCache{
		stmts: make(map[string]*sql.Stmt),
		db:    db,
	}
}

func (c *memStatementCache) Prepare(query string) (*sql.Stmt, error) {
	c.mu.RLock()
	if stmt, ok := c.stmts[query]; ok {
		c.mu.RUnlock()
		return stmt, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if stmt, ok := c.stmts[query]; ok {
		return stmt, nil
	}

	stmt, err := c.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	c.stmts[query] = stmt
	return stmt, nil
}

func (c *memStatementCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stmt := range c.stmts {
		stmt.Close()
	}
	c.stmts = nil
	return nil
}
