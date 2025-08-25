package database

import (
	"context"
	"database/sql"
)

type Database interface {
	Query(query string, args ...interface{}) (Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error)
	PingContext(ctx context.Context) error
	Close() error
	SetMaxOpenConns(n int)
	SetMaxIdleConns(n int)
	Prepare(query string) (*sql.Stmt, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Columns() ([]string, error)
}
