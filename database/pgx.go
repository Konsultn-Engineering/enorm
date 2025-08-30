package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgxDatabase implements Database for pgxpool.Pool.
type PgxDatabase struct {
	pool *pgxpool.Pool
}

// NewPgxDatabase creates a new PgxDatabase.
func NewPgxDatabase(pool *pgxpool.Pool) *PgxDatabase {
	return &PgxDatabase{pool: pool}
}

// Query executes a query that returns rows.
func (p *PgxDatabase) Query(query string, args ...any) (Rows, error) {
	rows, err := p.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxRows{rows: rows}, nil
}

// QueryContext executes a query with a context.
func (p *PgxDatabase) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxRows{rows: rows}, nil
}

// Exec executes a query without returning rows.
func (p *PgxDatabase) Exec(query string, args ...any) (Result, error) {
	cmdTag, err := p.pool.Exec(context.Background(), query, args...)
	return &PgxResult{cmdTag: cmdTag}, err
}

// PingContext verifies the connection to the database is alive.
func (p *PgxDatabase) PingContext(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// Close closes the database.
func (p *PgxDatabase) Close() error {
	p.pool.Close()
	return nil
}

// SetMaxOpenConns is a no-op for pgxpool.
func (p *PgxDatabase) SetMaxOpenConns(n int) {}

// SetMaxIdleConns is a no-op for pgxpool.
func (p *PgxDatabase) SetMaxIdleConns(n int) {}

// Prepare is not supported in pgxpool.
func (p *PgxDatabase) Prepare(query string) (*sql.Stmt, error) {
	return nil, fmt.Errorf("Prepare not supported with pgxpool - queries are automatically prepared")
}

// PgxRows implements Rows for pgx.Rows.
type PgxRows struct {
	rows              pgx.Rows
	fieldDescriptions []pgconn.FieldDescription
}

// Next prepares the next result row for reading.
func (p *PgxRows) Next() bool { return p.rows.Next() }

// Scan copies the columns from the current row into the provided destinations.
func (p *PgxRows) Scan(dest ...any) error { return p.rows.Scan(dest...) }

// Close closes the rows iterator.
func (p *PgxRows) Close() error { p.rows.Close(); return nil }

// Columns returns the column names.
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

// Values returns the values for the current row.
func (p *PgxRows) Values() ([]any, error) {
	return p.rows.Values()
}

// PgxResult implements Result for pgxpool command tags.
type PgxResult struct {
	cmdTag pgconn.CommandTag
}

// LastInsertId is not supported in PostgreSQL with pgxpool.
func (r *PgxResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId not supported in PostgreSQL")
}

// RowsAffected returns the number of rows affected by the command.
func (r *PgxResult) RowsAffected() (int64, error) {
	return r.cmdTag.RowsAffected(), nil
}

// Assert that PgxDatabase implements the Database interface.
var _ Database = (*PgxDatabase)(nil)
