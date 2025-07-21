package engine

import "database/sql"

type Engine struct {
	db *sql.DB
}

func New(db *sql.DB) *Engine {
	return &Engine{db: db}
}

func (e *Engine) Where(condition string, args ...any) *Session {
	return &Session{
		engine: e,
		where:  condition,
		args:   args,
	}
}

func (e *Engine) GetRawDB() *sql.DB {
	return e.db
}
