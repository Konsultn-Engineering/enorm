package connector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

// Keep your existing interfaces and config structs exactly as-is
type Connection interface {
	DB() *sql.DB
	Database() database.Database
	Dialect() dialect.Dialect
	Health(ctx context.Context) error
	Stats() ConnectionStats
	Close() error
}

// Replace the registration pattern with a simple factory
func New(driver string, cfg Config) (Connection, error) {
	switch driver {
	case "postgres":
		return newPostgresConnector(cfg)
	case "mysql":
		//return newMySQLConnector(cfg), nil // future
	case "sqlite":
		//return newSQLiteConnector(cfg), nil // future
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
	return nil, fmt.Errorf("unsupported driver: %s", driver)
}
