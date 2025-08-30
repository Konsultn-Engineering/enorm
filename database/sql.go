package database

import (
	"context"
	"database/sql"
)

// SqlDatabase implements Database for *sql.DB.
type SqlDatabase struct {
	db *sql.DB
}

// NewSqlDatabase creates a new SqlDatabase.
func NewSqlDatabase(db *sql.DB) *SqlDatabase {
	return &SqlDatabase{db: db}
}

// Query executes a query that returns rows.
func (s *SqlDatabase) Query(query string, args ...any) (Rows, error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &SqlRows{rows: rows}, nil
}

// QueryContext executes a query with a context.
func (s *SqlDatabase) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SqlRows{rows: rows}, nil
}

// Exec executes a query without returning rows.
func (s *SqlDatabase) Exec(query string, args ...any) (Result, error) {
	res, err := s.db.Exec(query, args...)
	return res, err // database/sql.Result implements Result
}

// PingContext verifies the connection to the database is alive.
func (s *SqlDatabase) PingContext(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database.
func (s *SqlDatabase) Close() error { return s.db.Close() }

// SetMaxOpenConns sets the maximum number of open connections.
func (s *SqlDatabase) SetMaxOpenConns(n int) { s.db.SetMaxOpenConns(n) }

// SetMaxIdleConns sets the maximum number of idle connections.
func (s *SqlDatabase) SetMaxIdleConns(n int) { s.db.SetMaxIdleConns(n) }

// Prepare creates a prepared statement for later queries or executions.
func (s *SqlDatabase) Prepare(query string) (*sql.Stmt, error) { return s.db.Prepare(query) }

// SqlRows implements Rows for *sql.Rows.
type SqlRows struct {
	rows *sql.Rows
}

// Next prepares the next result row for reading.
func (s *SqlRows) Next() bool { return s.rows.Next() }

// Scan copies the columns from the current row into the provided destinations.
func (s *SqlRows) Scan(dest ...any) error { return s.rows.Scan(dest...) }

// Close closes the rows iterator.
func (s *SqlRows) Close() error { return s.rows.Close() }

// Columns returns the column names.
func (s *SqlRows) Columns() ([]string, error) { return s.rows.Columns() }

// Values returns the values for the current row.
// Note: database/sql does not provide this directly; returns empty slice.
func (s *SqlRows) Values() ([]any, error) { return []any{}, nil }

// Assert that SqlDatabase implements the Database interface.
var _ Database = (*SqlDatabase)(nil)
