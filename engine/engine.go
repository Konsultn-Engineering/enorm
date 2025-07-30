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
	"time"
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

	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols([]string{"id"}), // or build dynamically from meta.Fields
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

	// Create temporary holders for scanned values
	columns := []string{"id"} // this should match astFromCols
	scanVals := make([]any, len(columns))
	for i := range scanVals {
		var tmp any
		scanVals[i] = &tmp
	}

	// Perform the scan
	err = rows.Scan(scanVals...)
	if err != nil {
		return "", err
	}

	// Apply scanned values using DirectSet
	structVal := reflect.ValueOf(dest).Elem()
	structPtr := unsafe.Pointer(structVal.UnsafeAddr())

	for i, col := range columns {
		fieldMeta := meta.SnakeMap[col]
		if fieldMeta == nil {
			continue // ignore unmapped fields
		}

		raw := *(scanVals[i].(*any))

		switch fieldMeta.Type.Kind() {
		case reflect.Uint64:
			if v, ok := raw.(int64); ok {
				tmp := uint64(v)
				fieldMeta.DirectSet(structPtr, &tmp)
				continue
			}
		case reflect.Int64:
			if v, ok := raw.(int64); ok {
				fieldMeta.DirectSet(structPtr, &v)
				continue
			}
		case reflect.String:
			if v, ok := raw.(string); ok {
				fieldMeta.DirectSet(structPtr, &v)
				continue
			}
		case reflect.Struct:
			if fieldMeta.Type.String() == "time.Time" {
				if v, ok := raw.(time.Time); ok {
					fieldMeta.DirectSet(structPtr, &v)
					continue
				}
			}
		}

		fieldMeta.DirectSet(structPtr, &raw)
	}

	return query, nil
}

func ptr[T any](v T) *T { return &v }
