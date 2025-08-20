package connector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/engine"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// Postgres connector implementation
type postgresConnector struct {
	config Config
}

func newPostgresConnector(cfg Config) Connector {
	return &postgresConnector{config: cfg}
}

func (p *postgresConnector) buildDSN() string {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?",
		p.config.Username, p.config.Password, p.config.Host, p.config.Port, p.config.Database)

	if p.config.SSLMode != "" {
		dsn += "sslmode=" + p.config.SSLMode + "&"
	}

	// Add any additional params
	for key, value := range p.config.Params {
		dsn += fmt.Sprintf("%s=%s&", key, value)
	}

	return dsn
}

func (p *postgresConnector) Connect(ctx context.Context) (Connection, error) {
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
		return nil, err
	}

	poolCfg.MaxConns = int32(cfg.Pool.MaxOpen)
	poolCfg.MinConns = int32(cfg.Pool.MaxIdle)
	poolCfg.MaxConnLifetime = cfg.Pool.MaxLifetime
	poolCfg.MaxConnIdleTime = cfg.Pool.MaxIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	return &postgresConnection{
		pool:    pool,
		dialect: dialect.NewPostgresDialect(),
	}, nil
}

func (p *postgresConnector) ConnectWithRetry(ctx context.Context, opts RetryOptions) (Connection, error) {
	return retryConnect(ctx, opts, p.Connect)
}

func (p *postgresConnector) Close() error {
	// Nothing to close at connector level
	return nil
}

// Postgres connection implementation
type postgresConnection struct {
	pool    *pgxpool.Pool
	dialect dialect.Dialect
}

func (c *postgresConnection) DB() *sql.DB {
	return stdlib.OpenDBFromPool(c.pool)
}

// NEW: Direct engine creation for optimal performance
func (c *postgresConnection) Engine() *engine.Engine {
	return engine.NewWithPgx(c.pool)
}

func (c *postgresConnection) Dialect() dialect.Dialect {
	return c.dialect
}

func (c *postgresConnection) Health(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

func (c *postgresConnection) Stats() ConnectionStats {
	s := c.pool.Stat()
	return ConnectionStats{
		OpenConnections: int(s.TotalConns()),
		InUse:           int(s.AcquiredConns()),
		Idle:            int(s.IdleConns()),
	}
}

func (c *postgresConnection) Close() error {
	c.pool.Close()
	return nil
}
