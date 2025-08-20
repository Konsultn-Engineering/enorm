package engine

import (
	"context"
	"database/sql"
)

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
