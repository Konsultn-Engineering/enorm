package engine

import (
	"context"
)

type Database interface {
	Query(query string, args ...interface{}) (Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
	PingContext(ctx context.Context) error
	Close() error
	SetMaxOpenConns(n int)
	SetMaxIdleConns(n int)
}

type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Columns() ([]string, error)
}
