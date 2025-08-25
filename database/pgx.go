package database

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgxDatabase struct {
	pool *pgxpool.Pool
}

func NewPgxDatabase(pool *pgxpool.Pool) *PgxDatabase {
	return &PgxDatabase{pool: pool}
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

func (p *PgxDatabase) Prepare(query string) (*sql.Stmt, error) {
	return nil, fmt.Errorf("Prepare not supported with pgxpool - queries are automatically prepared")
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
