package connector

import (
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TODO: Add file for each connector type (Postgres, MySQL, SQLite) to keep this file clean
type DBType string

const (
	Postgres DBType = "postgres"
	MySQL    DBType = "mysql"
	SQLite   DBType = "sqlite"
)

type ConnectionParams struct {
	Type     DBType
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func (p ConnectionParams) DriverAndDSN() (driver string, dsn string, err error) {
	switch p.Type {
	case Postgres:
		ssl := p.SSLMode
		if ssl == "" {
			ssl = "disable"
		}
		return "pgx", fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s?sslmode=%s",
			p.User, p.Password, p.Host, p.Port, p.DBName, ssl,
		), nil
	case MySQL:
		return "mysql", fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s",
			p.User, p.Password, p.Host, p.Port, p.DBName,
		), nil
	case SQLite:
		return "sqlite3", p.DBName, nil
	default:
		return "", "", fmt.Errorf("unsupported db type: %s", p.Type)
	}
}

func Connect(p *ConnectionParams) (*sql.DB, error) {
	driver, dsn, err := p.DriverAndDSN()
	if err != nil {
		return nil, err
	}
	return sql.Open(driver, dsn)
}
