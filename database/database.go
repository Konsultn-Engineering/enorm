package database

import (
	"context"
	"database/sql"
)

type Database interface {
	Query(query string, args ...any) (Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
	PingContext(ctx context.Context) error
	Close() error
	SetMaxOpenConns(n int)
	SetMaxIdleConns(n int)
	Prepare(query string) (*sql.Stmt, error)
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Columns() ([]string, error)
	Values() ([]any, error)
}
