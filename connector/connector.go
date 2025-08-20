package connector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

// Connection represents a database connection with associated metadata.
type Connection interface {
	DB() *sql.DB
	Database() database.Database
	Dialect() dialect.Dialect
	Health(ctx context.Context) error
	Stats() ConnectionStats
	Close() error
}

// ClusterConnection represents a connection to a database cluster.
type ClusterConnection interface {
	Connection
	Primary() Connection
	Replicas() []Connection
	Read(ctx context.Context) Connection
	Write(ctx context.Context) Connection
}

// New creates a new database connection for the specified driver.
func New(driver string, cfg Config) (Connection, error) {
	switch driver {
	case "postgres":
		return newPostgresConnector(cfg)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
}

// NewCluster creates a new cluster connection for the specified driver.
func NewCluster(driver string, cfg ClusterConfig) (ClusterConnection, error) {
	if err := cfg.ValidateCluster(); err != nil {
		return nil, fmt.Errorf("invalid cluster config: %w", err)
	}

	switch driver {
	case "postgres":
		return newPostgresCluster(cfg)
	default:
		return nil, fmt.Errorf("unsupported driver for cluster: %s", driver)
	}
}
