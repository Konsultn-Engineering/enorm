package query

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"reflect"
	"sync"
	"unsafe"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/schema"
)

type SelectBuilder[T any] struct {
	stmt       *ast.SelectStmt
	tableName  string
	ctx        *schema.Context
	paramCount int

	// Stack-allocated parameter storage for common cases
	paramSlots [8]any
	paramHeap  []any // Only allocated when > 8 params
}

// Builder pool for zero-allocation creation
var selectBuilderPool = sync.Pool{
	New: func() any {
		return &SelectBuilder[any]{
			stmt: ast.NewSelectStmt(),
		}
	},
}

func Select[T any](ctx *schema.Context) *SelectBuilder[T] {
	// Get from pool (zero allocation)
	b := selectBuilderPool.Get().(*SelectBuilder[any])

	// Reset state
	b.paramCount = 0
	b.paramHeap = nil
	b.ctx = ctx

	// Get table name from schema metadata
	var zero T
	meta, _ := ctx.Introspect(reflect.TypeOf(zero))
	b.tableName = meta.TableName

	// Setup basic SELECT *
	b.stmt.Columns = ast.AllColumns()
	b.stmt.From = ast.NewTable(b.tableName)

	return (*SelectBuilder[T])(unsafe.Pointer(b))
}

func (b *SelectBuilder[T]) Release() {
	if b.stmt != nil {
		b.stmt.Release()
		b.stmt = nil
	}
	b.paramCount = 0
	b.paramHeap = nil
	selectBuilderPool.Put((*SelectBuilder[any])(unsafe.Pointer(b)))
}

// Method chaining
func (b *SelectBuilder[T]) Columns(columns ...string) *SelectBuilder[T] {
	// Release existing columns
	for _, col := range b.stmt.Columns {
		if releasable, ok := col.(interface{ Release() }); ok {
			releasable.Release()
		}
	}

	b.stmt.Columns = ast.Columns(columns...)
	return b
}

func (b *SelectBuilder[T]) Where(column string, value any) *SelectBuilder[T] {
	b.addParam(value)

	condition := ast.NewBinaryExpr(
		ast.NewColumn(column),
		"=",
		ast.NewValue(value),
	)

	if b.stmt.Where == nil {
		b.stmt.Where = ast.NewWhereClause(condition)
	} else {
		// Combine with AND
		existing := b.stmt.Where.Condition
		b.stmt.Where.Condition = ast.NewBinaryExpr(existing, "AND", condition)
	}

	return b
}

func (b *SelectBuilder[T]) WhereIn(column string, values []any) *SelectBuilder[T] {
	for _, v := range values {
		b.addParam(v)
	}

	condition := ast.NewBinaryExpr(
		ast.NewColumn(column),
		"IN",
		ast.NewArray(values),
	)

	if b.stmt.Where == nil {
		b.stmt.Where = ast.NewWhereClause(condition)
	} else {
		existing := b.stmt.Where.Condition
		b.stmt.Where.Condition = ast.NewBinaryExpr(existing, "AND", condition)
	}

	return b
}

func (b *SelectBuilder[T]) WhereLike(column string, pattern string) *SelectBuilder[T] {
	b.addParam(pattern)

	condition := ast.NewBinaryExpr(
		ast.NewColumn(column),
		"LIKE",
		ast.NewValue(pattern),
	)

	if b.stmt.Where == nil {
		b.stmt.Where = ast.NewWhereClause(condition)
	} else {
		existing := b.stmt.Where.Condition
		b.stmt.Where.Condition = ast.NewBinaryExpr(existing, "AND", condition)
	}

	return b
}

func (b *SelectBuilder[T]) OrderBy(column string, desc bool) *SelectBuilder[T] {
	orderClause := ast.NewOrderByClause(ast.NewColumn(column), desc)
	b.stmt.OrderBy = append(b.stmt.OrderBy, orderClause)
	return b
}

func (b *SelectBuilder[T]) OrderByAsc(column string) *SelectBuilder[T] {
	return b.OrderBy(column, false)
}

func (b *SelectBuilder[T]) OrderByDesc(column string) *SelectBuilder[T] {
	return b.OrderBy(column, true)
}

func (b *SelectBuilder[T]) Limit(limit int) *SelectBuilder[T] {
	b.stmt.Limit = ast.Limit(limit)
	return b
}

func (b *SelectBuilder[T]) LimitOffset(limit, offset int) *SelectBuilder[T] {
	b.stmt.Limit = ast.LimitOffset(limit, offset)
	return b
}

func (b *SelectBuilder[T]) InnerJoin(table string, leftCol, rightCol string) *SelectBuilder[T] {
	condition := ast.JoinOn(b.tableName, leftCol, table, rightCol)
	join := ast.InnerJoin(table, condition)
	b.stmt.Joins = append(b.stmt.Joins, join)
	return b
}

func (b *SelectBuilder[T]) LeftJoin(table string, leftCol, rightCol string) *SelectBuilder[T] {
	condition := ast.JoinOn(b.tableName, leftCol, table, rightCol)
	join := ast.LeftJoin(table, condition)
	b.stmt.Joins = append(b.stmt.Joins, join)
	return b
}

// Parameter management
func (b *SelectBuilder[T]) addParam(value any) {
	if b.paramCount < 8 {
		b.paramSlots[b.paramCount] = value
	} else {
		if b.paramHeap == nil {
			b.paramHeap = make([]any, 0, 16)
		}
		b.paramHeap = append(b.paramHeap, value)
	}
	b.paramCount++
}

func (b *SelectBuilder[T]) getParams() []any {
	if b.paramCount <= 8 {
		return b.paramSlots[:b.paramCount]
	}

	params := make([]any, b.paramCount)
	copy(params, b.paramSlots[:8])
	copy(params[8:], b.paramHeap)
	return params
}

func (b *SelectBuilder[T]) Get() {
	fmt.Println(b.stmt) // For debugging, replace with actual execution logic
}

// // Execution methods
func (b *SelectBuilder[T]) ToSQL() (string, []any, error) {
	v := visitor.NewSQLVisitor(dialect.NewPostgresDialect(), cache.NewQueryCache())
	defer v.Release()

	sql, args, err := v.Build(b.stmt)
	if err != nil {
		return "", nil, err
	}

	return sql, args, nil
}

//
//func (b *SelectBuilder[T]) First(ctx context.Context) (T, error) {
//	var zero T
//
//	// Add LIMIT 1 for single record queries
//	originalLimit := b.stmt.Limit
//	if b.stmt.Limit == nil {
//		b.stmt.Limit = ast.Limit(1)
//	}
//
//	sql, args, err := b.ToSQL()
//	if err != nil {
//		return zero, err
//	}
//
//	// Restore original limit
//	if originalLimit == nil && b.stmt.Limit != nil {
//		b.stmt.Limit.Release()
//		b.stmt.Limit = originalLimit
//	}
//
//	// Execute query using your existing database connection
//	result, err := b.ctx.QueryOne(ctx, sql, args...)
//	if err != nil {
//		return zero, err
//	}
//
//	return result.(T), nil
//}
//
//func (b *SelectBuilder[T]) FindAll(ctx context.Context) ([]T, error) {
//	sql, args, err := b.ToSQL()
//	if err != nil {
//		return nil, err
//	}
//
//	// Execute query using your existing database connection
//	results, err := b.ctx.QueryAll(ctx, sql, args...)
//	if err != nil {
//		return nil, err
//	}
//
//	// Convert to typed slice
//	typedResults := make([]T, len(results))
//	for i, result := range results {
//		typedResults[i] = result.(T)
//	}
//
//	return typedResults, nil
//}
//
//func (b *SelectBuilder[T]) Count(ctx context.Context) (int64, error) {
//	// Save original columns
//	originalColumns := b.stmt.Columns
//
//	// Replace with COUNT(*)
//	b.stmt.Columns = []ast.Node{
//		&ast.Function{
//			Name: "COUNT",
//			Args: []ast.Node{ast.NewColumn("*")},
//		},
//	}
//
//	sql, args, err := b.ToSQL()
//	if err != nil {
//		return 0, err
//	}
//
//	// Restore original columns
//	b.stmt.Columns = originalColumns
//
//	// Execute count query
//	result, err := b.ctx.QueryScalar(ctx, sql, args...)
//	if err != nil {
//		return 0, err
//	}
//
//	return result.(int64), nil
//}

//func (b *SelectBuilder[T]) Exists(ctx context.Context) (bool, error) {
//	count, err := b.Count(ctx)
//	return count > 0, err
//}
