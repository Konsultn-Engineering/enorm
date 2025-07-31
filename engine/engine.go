package engine

import (
	"database/sql"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

type Engine struct {
	db          *sql.DB
	qcache      cache.QueryCache
	selectCache sync.Map  // map[reflect.Type]string
	addrsPool   sync.Pool // []any
	astCache    sync.Map  // map[string]*ast.SelectStmt
	colCache    sync.Map  // map[string][]ast.Node
	stmtCache   cache.StatementCache
	visitor     *visitor.SQLVisitor
}

func New(db *sql.DB) *Engine {
	qc := cache.NewQueryCache()
	schema.New(1024, func(key reflect.Type, value *schema.EntityMeta) {})
	return &Engine{
		db:        db,
		qcache:    qc,
		stmtCache: cache.NewStatementCache(db),
		addrsPool: sync.Pool{
			New: func() any {
				return make([]any, 0, 32)
			},
		},
		visitor: visitor.NewSQLVisitor(dialect.NewPostgresDialect(), qc),
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

	cols := []string{"id", "first_name", "email", "created_at", "updated_at"}

	// Pre-resolve setters once (eliminates runtime map lookups)
	setters := make([]func(unsafe.Pointer, any), len(cols))
	for i, col := range cols {
		if fieldMeta, exists := meta.ColumnMap[col]; exists {
			setters[i] = fieldMeta.DirectSet
		}
	}

	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols(cols),
		From:    &ast.Table{Name: meta.TableName},
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

	// Optimized scanning - single allocation
	scanVals := make([]any, len(cols))
	scanPtrs := make([]any, len(cols))
	for i := range cols {
		scanPtrs[i] = &scanVals[i]
	}

	err = rows.Scan(scanPtrs...)
	if err != nil {
		return "", err
	}

	// Fast struct pointer extraction (eliminates reflection)
	structPtr := unsafe.Pointer(reflect.ValueOf(dest).Pointer())

	// Direct setting with pre-resolved setters (no lookups)
	for i, setter := range setters {
		if setter != nil {
			setter(structPtr, scanVals[i])
		}
	}

	return query, nil
}

func ptr[T any](v T) *T { return &v }
