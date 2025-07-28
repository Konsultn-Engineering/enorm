package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type Provider struct{}

func init() {
	connector.Register("postgres", &Provider{})
}

func (p *Provider) buildDSN(cfg connector.Config) string {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	if cfg.SSLMode != "" {
		dsn += "?sslmode=" + cfg.SSLMode
	}
	return dsn
}

func (p *Provider) Connect(ctx context.Context, cfg connector.Config) (connector.Connection, error) {
	dsn := p.buildDSN(cfg)

	// apply defaults
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

	return &connection{pool: pool, dialect: dialect.NewPostgresDialect()}, nil
}

func (p *Provider) Dialect() dialect.Dialect {
	return dialect.NewPostgresDialect()
}

func (p *Provider) HealthCheck(ctx context.Context, conn connector.Connection) error {
	return conn.Health(ctx)
}

type connection struct {
	pool    *pgxpool.Pool
	dialect dialect.Dialect
}

func (c *connection) DB() *sql.DB {
	return stdlib.OpenDBFromPool(c.pool)
}

func (c *connection) Dialect() dialect.Dialect {
	return c.dialect
}

func (c *connection) Health(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

func (c *connection) Stats() connector.ConnectionStats {
	s := c.pool.Stat()
	return connector.ConnectionStats{
		OpenConnections: int(s.TotalConns()),
		InUse:           int(s.AcquiredConns()),
		Idle:            int(s.IdleConns()),
	}
}

func (c *connection) Close() error {
	c.pool.Close()
	return nil
}
