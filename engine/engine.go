package engine

import (
	"database/sql"
	"fmt"
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

	// create cache key
	//fixedKey := cache.GenerateFixedKey(cache.MethodFindOne, meta.Index)

	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols([]string{"id"}),
		From:    &ast.Table{Name: meta.Plural},
		Limit:   &ast.LimitClause{Count: ptr(1)},
	}

	if selectStmt == nil || selectStmt.From == nil || len(selectStmt.Columns) == 0 {
		panic("selectStmt is malformed")
	}

	//defer visitor.Release()

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

	columns := []string{"id"} // same as in astFromCols input
	addrs := e.addrsPool.Get().([]any)[:len(columns)]
	for i := range columns {
		var v any
		addrs[i] = &v
	}

	scanner := meta.ScannerFn

	if scanner == nil {
		return "", fmt.Errorf("no scanner registered for type %s", meta.Type.Name())
	}

	err = scanner(dest, rows)
	if err != nil {
		return "", fmt.Errorf("failed to scan result: %w", err)
	} /// some logic to execute the query

	err = rows.Scan(addrs...)

	if err != nil {
		return "", err
	}

	return "", nil
}

func ptr[T any](v T) *T { return &v }
