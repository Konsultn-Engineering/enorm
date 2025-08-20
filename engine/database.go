package engine

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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

// Standard database/sql wrapper
type SqlDatabase struct {
	db *sql.DB
}

func (s *SqlDatabase) Query(query string, args ...interface{}) (Rows, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &SqlRows{rows: rows}, nil
}

func (s *SqlDatabase) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SqlRows{rows: rows}, nil
}

func (s *SqlDatabase) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SqlDatabase) Close() error {
	return s.db.Close()
}

func (s *SqlDatabase) SetMaxOpenConns(n int) {
	s.db.SetMaxOpenConns(n)
}

func (s *SqlDatabase) SetMaxIdleConns(n int) {
	s.db.SetMaxIdleConns(n)
}

type SqlRows struct {
	rows *sql.Rows
}

func (s *SqlRows) Next() bool                     { return s.rows.Next() }
func (s *SqlRows) Scan(dest ...interface{}) error { return s.rows.Scan(dest...) }
func (s *SqlRows) Close() error                   { return s.rows.Close() }
func (s *SqlRows) Columns() ([]string, error)     { return s.rows.Columns() }

// Direct pgx implementation for optimal performance
type PgxDatabase struct {
	pool *pgxpool.Pool
}

func (p *PgxDatabase) Query(query string, args ...interface{}) (Rows, error) {
	rows, err := p.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxRows{rows: rows}, nil
}

func (p *PgxDatabase) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxRows{rows: rows}, nil
}

func (p *PgxDatabase) PingContext(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *PgxDatabase) Close() error {
	p.pool.Close()
	return nil
}

func (p *PgxDatabase) SetMaxOpenConns(n int) {
	// pgxpool handles this internally
}

func (p *PgxDatabase) SetMaxIdleConns(n int) {
	// pgxpool handles this internally
}

type PgxRows struct {
	rows              pgx.Rows
	fieldDescriptions []pgconn.FieldDescription
}

func (p *PgxRows) Next() bool                     { return p.rows.Next() }
func (p *PgxRows) Scan(dest ...interface{}) error { return p.rows.Scan(dest...) }
func (p *PgxRows) Close() error                   { p.rows.Close(); return nil }
func (p *PgxRows) Columns() ([]string, error) {
	if p.fieldDescriptions == nil {
		p.fieldDescriptions = p.rows.FieldDescriptions()
	}
	columns := make([]string, len(p.fieldDescriptions))
	for i, fd := range p.fieldDescriptions {
		columns[i] = fd.Name
	}
	return columns, nil
}
