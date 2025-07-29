package engine

import (
	"database/sql"
	"errors"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"reflect"
	"sync"
)

type Engine struct {
	db          *sql.DB
	visitor     *visitor.SQLVisitor
	selectCache sync.Map  // map[reflect.Type]string
	addrsPool   sync.Pool // []any
}

func New(db *sql.DB) *Engine {
	v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	return &Engine{
		db:      db,
		visitor: v,
		addrsPool: sync.Pool{
			New: func() any {
				return make([]any, 0, 32)
			},
		},
	}
}

func (e *Engine) astFromCols(cols []string) []ast.Node {
	var result []ast.Node

	for _, col := range cols {
		result = append(result, &ast.Column{Name: col})
	}

	return result
}

func (e *Engine) FindOne(dest any) (string, error) {
	meta, err := schema.Introspect(reflect.TypeOf(dest))

	if err != nil {
		return "", err
	}

	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
		From:    &ast.Table{Name: meta.Plural},
		Limit:   &ast.LimitClause{Count: ptr(1)},
	}

	query, _, err := e.visitor.Build(selectStmt)

	if err != nil {
		return "", err
	}

	rows, err := e.db.Query(query)

	if err != nil {
		return "", err
	}

	defer rows.Close()

	if !rows.Next() {
		return "", sql.ErrNoRows
	}

	columns := []string{"id", "first_name", "email", "created_at", "updated_at"} // same as in astFromCols input
	addrs := e.addrsPool.Get().([]any)[:len(columns)]
	for i := range columns {
		var v any
		addrs[i] = &v
	}

	err = rows.Scan(addrs...)

	if errors.Is(err, sql.ErrNoRows) {
		e.addrsPool.Put(addrs[:0])
		return query, nil // or return "", nil depending on intent
	}
	if err != nil {
		e.addrsPool.Put(addrs[:0])
		return "", err
	}

	if meta.ScannerFn != nil {
		err = meta.ScannerFn(dest, rows)
	}

	return query, err
}

func ptr[T any](v T) *T { return &v }
