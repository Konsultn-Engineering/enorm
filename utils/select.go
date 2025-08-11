package utils

//
//import (
//	"fmt"
//	"github.com/Konsultn-Engineering/enorm/context"
//	"github.com/Konsultn-Engineering/enorm/query"
//	"reflect"
//	"unsafe"
//
//	"github.com/Konsultn-Engineering/enorm/ast"
//	"github.com/Konsultn-Engineering/enorm/schema"
//)
//
//type SelectBuild[T any] struct {
//	stmt       *ast.SelectStmt
//	tableName  string
//	paramCount int
//
//	// Stack-allocated parameter storage for common cases
//	paramSlots [8]any
//	paramHeap  []any // Only allocated when > 8 params
//
//	// Track column slice for proper cleanup
//	columnSlice []ast.Node
//	visitor     ast.Visitor
//	schema      *schema.Context
//}
//
//// Builder pool for zero-allocation creation
//
//func Select[T any](context context.ConnectionContext) *SelectBuild[T] {
//	// Get from pool (zero allocation)
//	b := query.selectBuilderPool.Get().(*SelectBuild[any])
//
//	b.visitor = context.GetVisitor()
//	b.schema = context.GetSchema() // Reset state
//	b.paramCount = 0
//	b.paramHeap = nil
//
//	// Get table name from schema metadata
//	var zero T
//	meta, _ := b.schema.Introspect(reflect.TypeOf(zero))
//	b.tableName = meta.TableName
//
//	// Setup basic SELECT * with pooled slice
//	b.columnSlice = ast.AllColumns()
//	b.stmt.Columns = b.columnSlice
//	b.stmt.From = ast.NewTable(b.tableName)
//
//	return (*SelectBuild[T])(b)
//}
//
//func (b *SelectBuild[T]) Release() {
//	if b.stmt != nil {
//		b.stmt.Release()
//		b.stmt = nil
//	}
//
//	// Release column slice if we have one
//	if b.columnSlice != nil {
//		ast.ReleaseNodeSlice(b.columnSlice)
//		b.columnSlice = nil
//	}
//
//	b.paramCount = 0
//	b.paramHeap = nil
//
//	query.selectBuilderPool.Put((*SelectBuild[any])(unsafe.Pointer(b)))
//}
//
//// Method chaining
//func (b *SelectBuild[T]) Columns(columns ...string) *SelectBuild[T] {
//	// Release existing column slice
//	if b.columnSlice != nil {
//		ast.ReleaseNodeSlice(b.columnSlice)
//	}
//
//	// Create new pooled slice
//	b.columnSlice = ast.Columns(columns...)
//	b.stmt.Columns = b.columnSlice
//	return b
//}
//
//func (b *SelectBuild[T]) Where(column string, value any) *SelectBuild[T] {
//	b.addParam(value)
//
//	condition := ast.NewBinaryExpr(
//		ast.NewColumn(column),
//		"=",
//		ast.NewValue(value),
//	)
//
//	// Let AST handle the condition management internally
//	b.stmt.AddWhereCondition(condition, "AND")
//	return b
//}
//
//func (b *SelectBuild[T]) WhereOr(column string, value any) *SelectBuild[T] {
//	b.addParam(value)
//
//	condition := ast.NewBinaryExpr(
//		ast.NewColumn(column),
//		"=",
//		ast.NewValue(value),
//	)
//
//	// Let AST handle the condition management internally
//	b.stmt.AddWhereCondition(condition, "OR")
//	return b
//}
//
//func (b *SelectBuild[T]) WhereIn(column string, values []any) *SelectBuild[T] {
//	for _, v := range values {
//		b.addParam(v)
//	}
//
//	condition := ast.NewBinaryExpr(
//		ast.NewColumn(column),
//		"IN",
//		ast.NewArray(values),
//	)
//
//	b.stmt.AddWhereCondition(condition, "AND")
//	return b
//}
//
//func (b *SelectBuild[T]) WhereLike(column string, pattern string) *SelectBuild[T] {
//	b.addParam(pattern)
//
//	condition := ast.NewBinaryExpr(
//		ast.NewColumn(column),
//		"LIKE",
//		ast.NewValue(pattern),
//	)
//
//	b.stmt.AddWhereCondition(condition, "AND")
//	return b
//}
//
//func (b *SelectBuild[T]) OrderBy(column string, desc bool) *SelectBuild[T] {
//	orderClause := ast.NewOrderByClause(ast.NewColumn(column), desc)
//	b.stmt.OrderBy = append(b.stmt.OrderBy, orderClause)
//	return b
//}
//
//func (b *SelectBuild[T]) OrderByAsc(column string) *SelectBuild[T] {
//	return b.OrderBy(column, false)
//}
//
//func (b *SelectBuild[T]) OrderByDesc(column string) *SelectBuild[T] {
//	return b.OrderBy(column, true)
//}
//
//func (b *SelectBuild[T]) Limit(limit int) *SelectBuild[T] {
//	b.stmt.Limit = ast.Limit(limit)
//	return b
//}
//
//func (b *SelectBuild[T]) LimitOffset(limit, offset int) *SelectBuild[T] {
//	b.stmt.Limit = ast.LimitOffset(limit, offset)
//	return b
//}
//
//func (b *SelectBuild[T]) InnerJoin(table string, leftCol, rightCol string) *SelectBuild[T] {
//	condition := ast.JoinOn(b.tableName, leftCol, table, rightCol)
//	join := ast.InnerJoin(table, condition)
//	b.stmt.Joins = append(b.stmt.Joins, join)
//	return b
//}
//
//func (b *SelectBuild[T]) LeftJoin(table string, leftCol, rightCol string) *SelectBuild[T] {
//	condition := ast.JoinOn(b.tableName, leftCol, table, rightCol)
//	join := ast.LeftJoin(table, condition)
//	b.stmt.Joins = append(b.stmt.Joins, join)
//	return b
//}
//
//// Parameter management
//func (b *SelectBuild[T]) addParam(value any) {
//	if b.paramCount < 8 {
//		b.paramSlots[b.paramCount] = value
//	} else {
//		if b.paramHeap == nil {
//			b.paramHeap = make([]any, 0, 16)
//		}
//		b.paramHeap = append(b.paramHeap, value)
//	}
//	b.paramCount++
//}
//
//func (b *SelectBuild[T]) getParams() []any {
//	if b.paramCount <= 8 {
//		return b.paramSlots[:b.paramCount]
//	}
//
//	params := make([]any, b.paramCount)
//	copy(params, b.paramSlots[:8])
//	copy(params[8:], b.paramHeap)
//	return params
//}
//
//func (b *SelectBuild[T]) Get() {
//	b.stmt.BuildWhere()
//	fmt.Println(b.stmt) // For debugging, replace with actual execution logic
//}
//
//// Execution methods
//func (b *SelectBuild[T]) ToSQL() (string, []any, error) {
//	b.stmt.BuildWhere() // Build WHERE clause before SQL generation
//
//	defer b.visitor.Release()
//
//	sql, args, err := b.visitor.Build(b.stmt)
//	if err != nil {
//		return "", nil, err
//	}
//
//	return sql, args, nil
//}
//
//func (b *SelectBuild[T]) FindOne() (*T, error) {
//	b.stmt.BuildWhere() // Build WHERE clause before execution
//	// Execute the query and return a single result
//	return nil, nil
//}
//
//func (b *SelectBuild[T]) Find() ([]*T, error) {
//	b.stmt.BuildWhere() // Build WHERE clause before execution
//	// Execute the query and return multiple results
//	return nil, nil
//}
