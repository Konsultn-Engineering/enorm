package engine

import (
	"context"
	"database/sql"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/query"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"reflect"
	"sync"
)

type Engine struct {
	*query.Builder
	db               database.Database
	schema           *schema.Context
	columnCache      sync.Map
	scanPool         sync.Pool
	queryStringCache map[string]string
	cacheMu          sync.RWMutex
}

func New(conn connector.Connection) *Engine {
	qc := cache.NewQueryCache()
	v := visitor.NewSQLVisitor(conn.Dialect(), qc)

	e := &Engine{
		Builder:          query.NewBuilder("", "", v),
		db:               conn.Database(),
		schema:           schema.New(),
		queryStringCache: make(map[string]string, 64),
	}

	e.scanPool = sync.Pool{
		New: func() interface{} {
			vals := make([]any, 8)
			ptrs := make([]any, 8)
			// PRE-COMPUTE: Set up pointers once
			for i := range ptrs {
				ptrs[i] = &vals[i]
			}
			return &struct {
				vals []any
				ptrs []any
			}{vals, ptrs}
		},
	}

	return e
}

// =============================================================================
// EXECUTION METHODS
// =============================================================================

func (e *Engine) FindOne(dest any) (string, error) {
	e.Limit(1)

	meta, err := e.schema.Introspect(reflect.TypeOf(dest))
	if err != nil {
		return "", err
	}

	query, args, err := e.Builder.Build(meta.TableName, meta.Columns)
	if err != nil {
		return "", err
	}

	rows, err := e.db.Query(query, args...)
	if err != nil {
		return query, err
	}
	defer rows.Close()

	if !rows.Next() {
		return query, sql.ErrNoRows
	}

	// REPLACE YOUR CURRENT SLICE ALLOCATION WITH THIS:
	scanRes := e.scanPool.Get().(*struct {
		vals []any
		ptrs []any
	})
	defer func() {
		e.scanPool.Put(scanRes)
	}()

	// Resize if needed (pointers already set up)
	colCount := len(meta.Columns)
	if cap(scanRes.vals) < colCount {
		// Need to reallocate and rebuild pointers
		scanRes.vals = make([]any, colCount)
		scanRes.ptrs = make([]any, colCount)
		for i := range scanRes.ptrs {
			scanRes.ptrs[i] = &scanRes.vals[i]
		}
	} else {
		// Just resize, pointers already correct
		scanRes.vals = scanRes.vals[:colCount]
		scanRes.ptrs = scanRes.ptrs[:colCount]
	}

	err = rows.Scan(scanRes.ptrs...)
	if err != nil {
		return query, err
	}

	err = meta.ScanAndSet(dest, meta.Columns, scanRes.vals)
	if err != nil {
		return query, err
	}

	return query, nil
}

// FindMany optimized for batch operations
func (e *Engine) FindMany(dest any, ids []any) (string, error) {
	return "", nil
}

func (e *Engine) FindAll(dest any) (string, error) {
	return "", nil
}

func (e *Engine) Exists() (bool, error) {
	return false, nil
}

// =============================================================================
// TABLE AND COLUMN SELECTION
// =============================================================================

func (e *Engine) Table(table string) *Engine {
	//e.Builder.GetStatement().From = ast.NewTable("", table, "")
	return e
}

func (e *Engine) Select(columns ...string) *Engine {
	e.Builder.Select(columns)
	return e
}

func (e *Engine) SelectRaw(expr string, alias ...string) *Engine {
	aliasStr := ""
	if len(alias) > 0 {
		aliasStr = alias[0]
	}
	e.Builder.SelectRaw(expr, aliasStr)
	return e
}

func (e *Engine) Distinct() *Engine {
	e.Builder.Distinct()
	return e
}

// =============================================================================
// WHERE CONDITIONS (AND)
// =============================================================================

func (e *Engine) Where(column string, operator string, value any) *Engine {
	e.Builder.Where(column, operator, value)
	return e
}

func (e *Engine) WhereEq(column string, value any) *Engine {
	return e.Where(column, ast.OpEqual, value)
}

func (e *Engine) WhereNotEq(column string, value any) *Engine {
	return e.Where(column, ast.OpNotEqual, value)
}

func (e *Engine) WhereIn(column string, values []any) *Engine {
	return e.Where(column, ast.OpIn, values)
}

func (e *Engine) WhereNotIn(column string, values []any) *Engine {
	return e.Where(column, ast.OpNotIn, values)
}

func (e *Engine) WhereLike(column string, pattern string) *Engine {
	return e.Where(column, ast.OpLike, pattern)
}

func (e *Engine) WhereNotLike(column string, pattern string) *Engine {
	return e.Where(column, ast.OpNotLike, pattern)
}

func (e *Engine) WhereGt(column string, value any) *Engine {
	return e.Where(column, ast.OpGreaterThan, value)
}

func (e *Engine) WhereGte(column string, value any) *Engine {
	return e.Where(column, ast.OpGreaterThanOrEqual, value)
}

func (e *Engine) WhereLt(column string, value any) *Engine {
	return e.Where(column, ast.OpLessThan, value)
}

func (e *Engine) WhereLte(column string, value any) *Engine {
	return e.Where(column, ast.OpLessThanOrEqual, value)
}

func (e *Engine) WhereIsNull(column string) *Engine {
	return e.Where(column, ast.OpIsNull, nil)
}

func (e *Engine) WhereIsNotNull(column string) *Engine {
	return e.Where(column, ast.OpIsNotNull, nil)
}

func (e *Engine) WhereBetween(column string, start, end any) *Engine {
	return e.Where(column, ast.OpBetween, []any{start, end})
}

func (e *Engine) WhereNotBetween(column string, start, end any) *Engine {
	return e.Where(column, ast.OpNotBetween, []any{start, end})
}

// =============================================================================
// WHERE CONDITIONS (OR)
// =============================================================================

func (e *Engine) OrWhere(column string, operator string, value any) *Engine {
	e.Builder.OrWhere(column, operator, value)
	return e
}

func (e *Engine) OrWhereEq(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpEqual, value)
}

func (e *Engine) OrWhereNotEq(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpNotEqual, value)
}

func (e *Engine) OrWhereIn(column string, values []any) *Engine {
	return e.OrWhere(column, ast.OpIn, values)
}

func (e *Engine) OrWhereNotIn(column string, values []any) *Engine {
	return e.OrWhere(column, ast.OpNotIn, values)
}

func (e *Engine) OrWhereLike(column string, pattern string) *Engine {
	return e.OrWhere(column, ast.OpLike, pattern)
}

func (e *Engine) OrWhereNotLike(column string, pattern string) *Engine {
	return e.OrWhere(column, ast.OpNotLike, pattern)
}

func (e *Engine) OrWhereGt(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpGreaterThan, value)
}

func (e *Engine) OrWhereGte(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpGreaterThanOrEqual, value)
}

func (e *Engine) OrWhereLt(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpLessThan, value)
}

func (e *Engine) OrWhereLte(column string, value any) *Engine {
	return e.OrWhere(column, ast.OpLessThanOrEqual, value)
}

func (e *Engine) OrWhereIsNull(column string) *Engine {
	return e.OrWhere(column, ast.OpIsNull, nil)
}

func (e *Engine) OrWhereIsNotNull(column string) *Engine {
	return e.OrWhere(column, ast.OpIsNotNull, nil)
}

func (e *Engine) OrWhereBetween(column string, start, end any) *Engine {
	return e.OrWhere(column, ast.OpBetween, []any{start, end})
}

func (e *Engine) OrWhereNotBetween(column string, start, end any) *Engine {
	return e.OrWhere(column, ast.OpNotBetween, []any{start, end})
}

// =============================================================================
// SUBQUERY CONDITIONS
// =============================================================================

func (e *Engine) WhereExists(subqueryFn func(*Engine)) *Engine {
	e.Builder.WhereExists(func(b *query.Builder) {
		subEngine := &Engine{Builder: b, db: e.db, schema: e.schema}
		subqueryFn(subEngine)
	})
	return e
}

func (e *Engine) WhereSubquery(column string, operator string, subqueryFn func(*Engine)) *Engine {
	e.Builder.WhereSubquery(column, operator, func(b *query.Builder) {
		subEngine := &Engine{Builder: b, db: e.db, schema: e.schema}
		subqueryFn(subEngine)
	})
	return e
}

// =============================================================================
// ORDERING
// =============================================================================

func (e *Engine) OrderBy(columns []string, desc bool) *Engine {
	e.Builder.OrderBy(columns, desc)
	return e
}

func (e *Engine) OrderByAsc(columns ...string) *Engine {
	return e.OrderBy(columns, false)
}

func (e *Engine) OrderByDesc(columns ...string) *Engine {
	return e.OrderBy(columns, true)
}

// =============================================================================
// PAGINATION
// =============================================================================

func (e *Engine) Limit(limit int) *Engine {
	e.Builder.Limit(limit)
	return e
}

func (e *Engine) Offset(offset int) *Engine {
	e.Builder.Offset(offset)
	return e
}

func (e *Engine) LimitOffset(limit, offset int) *Engine {
	e.Builder.Limit(limit)
	e.Builder.Offset(offset)
	return e
}

// =============================================================================
// JOINS
// =============================================================================

func (e *Engine) InnerJoin(table string, leftCol string, rightCol string) *Engine {
	return e.Join(ast.JoinInner, table, leftCol, ast.OpEqual, rightCol)
}

func (e *Engine) LeftJoin(table string, leftCol string, rightCol string) *Engine {
	return e.Join(ast.JoinLeft, table, leftCol, ast.OpEqual, rightCol)
}

func (e *Engine) RightJoin(table string, leftCol string, rightCol string) *Engine {
	return e.Join(ast.JoinRight, table, leftCol, ast.OpEqual, rightCol)
}

func (e *Engine) Join(joinType ast.JoinType, table string, leftCol string, operator string, rightCol string) *Engine {
	e.Builder.Join(joinType, table, leftCol, operator, rightCol)
	return e
}

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

func (e *Engine) Count(column ...string) *Engine {
	col := "*"
	if len(column) > 0 {
		col = column[0]
	}
	e.Builder.SelectRaw("COUNT("+col+")", "")
	return e
}

func (e *Engine) Sum(column string) *Engine {
	e.Builder.SelectRaw("SUM("+column+")", "")
	return e
}

func (e *Engine) Avg(column string) *Engine {
	e.Builder.SelectRaw("AVG("+column+")", "")
	return e
}

func (e *Engine) Min(column string) *Engine {
	e.Builder.SelectRaw("MIN("+column+")", "")
	return e
}

func (e *Engine) Max(column string) *Engine {
	e.Builder.SelectRaw("MAX("+column+")", "")
	return e
}

// =============================================================================
// DATABASE OPERATIONS
// =============================================================================

func (e *Engine) Health(ctx context.Context) error {
	return e.db.PingContext(ctx)
}

func (e *Engine) Close() error {
	return e.db.Close()
}

// =============================================================================
// HELPER METHODS
// =============================================================================
// SetConnectionPool allows optimizing the connection pool
func (e *Engine) SetConnectionPool(maxOpen, maxIdle int) {
	e.db.SetMaxOpenConns(maxOpen)
	e.db.SetMaxIdleConns(maxIdle)
}
