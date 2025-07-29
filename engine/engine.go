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
	"strings"
	"sync"
)

type Engine struct {
	db          *sql.DB
	qcache      cache.QueryCache
	selectCache sync.Map    // map[reflect.Type]string
	addrsPool   sync.Pool   // []any
	astCache    sync.Map    // map[string]*ast.SelectStmt
	colCache    sync.Map    // map[string][]ast.Node
	stmtCache   cache.StatementCache
}

func New(db *sql.DB) *Engine {
	qc := cache.NewQueryCache()
	return &Engine{
		db:        db,
		qcache:    qc,
		stmtCache: cache.NewStatementCache(db),
		addrsPool: sync.Pool{
			New: func() any {
				return make([]any, 0, 32)
			},
		},
	}
}

func (e *Engine) astFromCols(cols []string) []ast.Node {
	// Create a cache key from column names
	key := strings.Join(cols, ",")
	
	// Check cache first
	if cached, ok := e.colCache.Load(key); ok {
		return cached.([]ast.Node)
	}
	
	// Create new AST nodes
	result := make([]ast.Node, len(cols))
	for i, col := range cols {
		result[i] = &ast.Column{Name: col}
	}
	
	// Cache the result
	e.colCache.Store(key, result)
	return result
}

func (e *Engine) FindOne(dest any) (string, error) {
	meta, err := schema.Introspect(reflect.TypeOf(dest))

	if err != nil {
		return "", err
	}

	// Create a cache key for this query type
	cacheKey := "findone:" + meta.Plural
	
	// Check if we have cached AST for this query
	var selectStmt *ast.SelectStmt
	if cached, ok := e.astCache.Load(cacheKey); ok {
		selectStmt = cached.(*ast.SelectStmt)
	} else {
		// Build new AST and cache it
		selectStmt = &ast.SelectStmt{
			Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
			From:    &ast.Table{Name: meta.Plural},
			Limit:   &ast.LimitClause{Count: ptr(1)},
		}
		e.astCache.Store(cacheKey, selectStmt)
	}

	// Get visitor from pool
	v := visitor.NewSQLVisitor(dialect.Postgres{}, e.qcache)
	defer v.Release()
	
	query, _, err := v.Build(selectStmt)

	if err != nil {
		return "", err
	}

	// Use prepared statement cache for better performance
	stmt, err := e.stmtCache.Prepare(query)
	if err != nil {
		return "", err
	}

	rows, err := stmt.Query()

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
