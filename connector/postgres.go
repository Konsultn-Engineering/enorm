package connector

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// PostgresConnector represents a PostgreSQL database connection.
type PostgresConnector struct {
	config  Config
	pool    *pgxpool.Pool
	dialect dialect.Dialect
}

// PostgresCluster represents a PostgreSQL cluster with primary and replica connections.
type PostgresCluster struct {
	config   ClusterConfig
	primary  *PostgresConnector
	replicas []*PostgresConnector
	mu       sync.RWMutex
	readIdx  int
}

// newPostgresConnector creates a new PostgreSQL connector with auto-connection.
func newPostgresConnector(cfg Config) (*PostgresConnector, error) {
	p := &PostgresConnector{
		config:  cfg,
		dialect: dialect.NewPostgresDialect(),
	}

	ctx := context.Background()
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}

	if cfg.Retry != nil {
		err := retryConnect(ctx, cfg.Retry, p.connect)
		if err != nil {
			return nil, fmt.Errorf("failed to connect after %d retries: %w", cfg.Retry.MaxRetries, err)
		}
	} else {
		err := p.connect(ctx)
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

// newPostgresCluster creates a new PostgreSQL cluster connection.
func newPostgresCluster(cfg ClusterConfig) (*PostgresCluster, error) {
	// Create primary connection
	primary, err := newPostgresConnector(cfg.Primary)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to primary: %w", err)
	}

	// Create replica connections
	var replicas []*PostgresConnector
	for i, replicaCfg := range cfg.Replicas {
		replica, err := newPostgresConnector(replicaCfg)
		if err != nil {
			// Close previously created connections on failure
			primary.Close()
			for _, r := range replicas {
				r.Close()
			}
			return nil, fmt.Errorf("failed to connect to replica %d: %w", i, err)
		}
		replicas = append(replicas, replica)
	}

	return &PostgresCluster{
		config:   cfg,
		primary:  primary,
		replicas: replicas,
	}, nil
}

// connect establishes the PostgreSQL connection.
func (p *PostgresConnector) connect(ctx context.Context) error {
	if p.pool != nil {
		return nil // Already connected
	}

	dsn := p.buildDSN()
	cfg := p.config

	// Apply defaults
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

// buildDSN creates a PostgreSQL connection string.
func (p *PostgresConnector) buildDSN() string {
	return NewDSNBuilder("postgres").
		Auth(p.config.Username, p.config.Password).
		Host(p.config.Host, p.config.Port).
		Database(p.config.Database).
		Param("sslmode", p.config.SSLMode).
		Params(p.config.Params).
		Build()
}

// DB returns the underlying *sql.DB instance.
func (p *PostgresConnector) DB() *sql.DB {
	return stdlib.OpenDBFromPool(p.pool)
}

// Database returns a database abstraction interface.
func (p *PostgresConnector) Database() database.Database {
	return database.NewPgxDatabase(p.pool)
}

// Dialect returns the PostgreSQL dialect.
func (p *PostgresConnector) Dialect() dialect.Dialect {
	return p.dialect
}

// Health checks the connection health.
func (p *PostgresConnector) Health(ctx context.Context) error {
	if p.pool == nil {
		return fmt.Errorf("not connected")
	}
	return p.pool.Ping(ctx)
}

// Stats returns connection pool statistics.
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

// Close closes the connection pool.
func (p *PostgresConnector) Close() error {
	if p.pool != nil {
		p.pool.Close()
		p.pool = nil
	}
	return nil
}

// Cluster-specific methods

// Primary returns the primary connection.
func (pc *PostgresCluster) Primary() Connection {
	return pc.primary
}

// Replicas returns all replica connections.
func (pc *PostgresCluster) Replicas() []Connection {
	connections := make([]Connection, len(pc.replicas))
	for i, replica := range pc.replicas {
		connections[i] = replica
	}
	return connections
}

// Read returns a connection for read operations based on the configured strategy.
func (pc *PostgresCluster) Read(ctx context.Context) Connection {
	if len(pc.replicas) == 0 {
		return pc.primary
	}

	switch pc.config.ReadStrategy {
	case "primary", "":
		return pc.primary
	case "random":
		return pc.replicas[rand.Intn(len(pc.replicas))]
	case "round_robin":
		pc.mu.Lock()
		idx := pc.readIdx % len(pc.replicas)
		pc.readIdx++
		pc.mu.Unlock()
		return pc.replicas[idx]
	default:
		return pc.primary
	}
}

// Write returns a connection for write operations (always primary).
func (pc *PostgresCluster) Write(ctx context.Context) Connection {
	return pc.primary
}

// DB returns the primary database connection.
func (pc *PostgresCluster) DB() *sql.DB {
	return pc.primary.DB()
}

// Database returns the primary database interface.
func (pc *PostgresCluster) Database() database.Database {
	return pc.primary.Database()
}

// Dialect returns the PostgreSQL dialect.
func (pc *PostgresCluster) Dialect() dialect.Dialect {
	return pc.primary.Dialect()
}

// Health checks the health of all connections in the cluster.
func (pc *PostgresCluster) Health(ctx context.Context) error {
	if err := pc.primary.Health(ctx); err != nil {
		return fmt.Errorf("primary health check failed: %w", err)
	}

	for i, replica := range pc.replicas {
		if err := replica.Health(ctx); err != nil {
			return fmt.Errorf("replica %d health check failed: %w", i, err)
		}
	}

	return nil
}

// Stats returns aggregated statistics from all connections.
func (pc *PostgresCluster) Stats() ConnectionStats {
	primaryStats := pc.primary.Stats()

	for _, replica := range pc.replicas {
		replicaStats := replica.Stats()
		primaryStats.OpenConnections += replicaStats.OpenConnections
		primaryStats.InUse += replicaStats.InUse
		primaryStats.Idle += replicaStats.Idle
	}

	return primaryStats
}

// Close closes all connections in the cluster.
func (pc *PostgresCluster) Close() error {
	var lastErr error

	if err := pc.primary.Close(); err != nil {
		lastErr = err
	}

	for _, replica := range pc.replicas {
		if err := replica.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
