package sqlorm

import (
	"konsultn-api/pkg/sqlorm/connector"
	"konsultn-api/pkg/sqlorm/engine"
)

type ConnectionParams = connector.ConnectionParams
type DBType = connector.DBType

const (
	Postgres = connector.Postgres
	MySQL    = connector.MySQL
	SQLite   = connector.SQLite
)

func Connect(p *ConnectionParams) (*engine.Engine, error) {
	db, err := connector.Connect(p)
	if err != nil {
		return nil, err
	}
	return engine.New(db), nil
}
