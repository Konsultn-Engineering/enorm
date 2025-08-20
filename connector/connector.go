package connector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/engine"
)

// Keep your existing interfaces and config structs exactly as-is
type Connection interface {
	DB() *sql.DB
	Engine() *engine.Engine // Add this method
	Dialect() dialect.Dialect
	Health(ctx context.Context) error
	Stats() ConnectionStats
	Close() error
}

type Connector interface {
	Connect(ctx context.Context) (Connection, error)
	ConnectWithRetry(ctx context.Context, opts RetryOptions) (Connection, error)
	Close() error
}

// Replace the registration pattern with a simple factory
func New(driver string, cfg Config) (Connector, error) {
	switch driver {
	case "postgres":
		return newPostgresConnector(cfg), nil
	case "mysql":
		//return newMySQLConnector(cfg), nil // future
	case "sqlite":
		//return newSQLiteConnector(cfg), nil // future
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
	return nil, fmt.Errorf("unsupported driver: %s", driver)
}
