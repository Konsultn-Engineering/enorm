package cache

import (
	"database/sql"
	"errors"
	lru "github.com/hashicorp/golang-lru/v2"
	"sync"
)

type StatementCache struct {
	cache *lru.Cache[uint64, *sql.Stmt]
	mu    sync.RWMutex
}

func NewStatementCache(size int) *StatementCache {
	cache, _ := lru.NewWithEvict(size, func(key uint64, stmt *sql.Stmt) {
		stmt.Close() // Clean up evicted statements
	})

	return &StatementCache{
		cache: cache,
	}
}

func (s *StatementCache) Get(key uint64) (*sql.Stmt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if stmt, ok := s.cache.Get(key); ok {
		return stmt, nil
	}
	return nil, errors.New("key not found")
}

func (s *StatementCache) Set(key uint64, stmt *sql.Stmt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache.Add(key, stmt)
	return nil
}

func (s *StatementCache) GetOrPrepare(key uint64, db *sql.DB, query string) (*sql.Stmt, error) {
	// Fast path: try to get from cache with read lock
	s.mu.RLock()
	if stmt, ok := s.cache.Get(key); ok {
		s.mu.RUnlock()
		return stmt, nil
	}
	s.mu.RUnlock()

	// Slow path: prepare and cache with write lock
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if stmt, ok := s.cache.Get(key); ok {
		return stmt, nil
	}

	// Prepare the statement
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}

	s.cache.Add(key, stmt)
	return stmt, nil
}

func (s *StatementCache) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache.Purge() // This will trigger the evict callback for all items
	return nil
}
