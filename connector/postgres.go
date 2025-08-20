package connector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"time"
)

// PostgresConnector handles both connection creation AND represents the connection
type PostgresConnector struct {
	config  Config
	pool    *pgxpool.Pool
	dialect dialect.Dialect
}

// NewPostgres creates a new postgres connector
func newPostgresConnector(cfg Config) (*PostgresConnector, error) {
	p := &PostgresConnector{
		config:  cfg,
		dialect: dialect.NewPostgresDialect(),
	}

	// Auto-connect with retry logic
	ctx := context.Background()
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	retryOpts := cfg.Retry
	err := retryConnect(ctx, *retryOpts, p.connect)
	if err != nil {
		return nil, fmt.Errorf("failed to connect after %d retries: %w", retryOpts.MaxRetries, err)
	}

	return p, nil
}

// Connect establishes the connection (idempotent)
func (p *PostgresConnector) connect(ctx context.Context) error {
	if p.pool != nil {
		return nil // Already connected
	}

	dsn := p.buildDSN()

	// Apply defaults
	cfg := p.config
	if cfg.Pool.MaxOpen <= 0 {
		cfg.Pool.MaxOpen = 10
	}
	if cfg.Pool.MaxIdle < 0 {
		cfg.Pool.MaxIdle = 5
	}
	if cfg.Pool.MaxLifetime == 0 {
		cfg.Pool.MaxLifetime = time.Hour
	}
	if cfg.Pool.MaxIdleTime == 0 {
		cfg.Pool.MaxIdleTime = 30 * time.Minute
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}

	poolCfg.MaxConns = int32(cfg.Pool.MaxOpen)
	poolCfg.MinConns = int32(cfg.Pool.MaxIdle)
	poolCfg.MaxConnLifetime = cfg.Pool.MaxLifetime
	poolCfg.MaxConnIdleTime = cfg.Pool.MaxIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}

	p.pool = pool
	return nil
}

// Connection interface methods
func (p *PostgresConnector) DB() *sql.DB {
	return stdlib.OpenDBFromPool(p.pool)
}

func (p *PostgresConnector) Database() database.Database {
	return database.NewPgxDatabase(p.pool)
}

func (p *PostgresConnector) Dialect() dialect.Dialect {
	return p.dialect
}

func (p *PostgresConnector) Health(ctx context.Context) error {
	if p.pool == nil {
		return fmt.Errorf("not connected")
	}
	return p.pool.Ping(ctx)
}

func (p *PostgresConnector) Stats() ConnectionStats {
	if p.pool == nil {
		return ConnectionStats{}
	}
	s := p.pool.Stat()
	return ConnectionStats{
		OpenConnections: int(s.TotalConns()),
		InUse:           int(s.AcquiredConns()),
		Idle:            int(s.IdleConns()),
	}
}

func (p *PostgresConnector) Close() error {
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
	return nil
}

func (p *PostgresConnector) buildDSN() string {
	return NewDSNBuilder("postgres").
		Auth(p.config.Username, p.config.Password).
		Host(p.config.Host, p.config.Port).
		Database(p.config.Database).
		Param("sslmode", p.config.SSLMode).
		Params(p.config.Params).
		Build()
}
