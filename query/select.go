package query

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"sync"
)

var (
	selectBuilderPool = sync.Pool{
		New: func() any {
			return &SelectBuilder{
				stmt: ast.NewSelectStmt(),
			}
		},
	}
)

type SelectBuilder struct {
	*BaseBuilder
	stmt        *ast.SelectStmt
	schema      string
	paramCount  int
	parent      *SelectBuilder // Points to parent builder
	firstChild  *SelectBuilder // Head of children linked list
	nextSibling *SelectBuilder // Next child in parent's list
}

// NewSelectBuilder creates a new SelectBuilder instance
func NewSelectBuilder(schema string, table string, visitor *visitor.SQLVisitor, executor Executor) *SelectBuilder {
	builder := selectBuilderPool.Get().(*SelectBuilder)

	// Initialize BaseBuilder
	builder.BaseBuilder = NewBaseBuilder(table, visitor, executor)

	// Reset statement
	builder.stmt = ast.NewSelectStmt()
	builder.schema = schema
	builder.paramCount = 0

	// Clear linked list pointers
	builder.parent = nil
	builder.firstChild = nil
	builder.nextSibling = nil

	if table != "" {
		builder.stmt.From = ast.NewTable(schema, table, "")
	}

	return builder
}

// Release returns the builder to the pool
func (sb *SelectBuilder) Release() {
	// Release all children first (walk linked list)
	child := sb.firstChild
	for child != nil {
		next := child.nextSibling
		child.Release()
		child = next
	}

	// Clear all links
	sb.firstChild = nil
	sb.parent = nil
	sb.nextSibling = nil

	// Release self
	if sb.stmt != nil {
		sb.stmt.Release()
	}
	sb.schema = ""
	sb.paramCount = 0
	selectBuilderPool.Put(sb)
}

// whereWithOperator is the private helper that handles all WHERE logic
func (sb *SelectBuilder) whereWithOperator(column string, sqlOp string, value any, logicalOp string) *SelectBuilder {
	var condition ast.Node

	switch sqlOp {
	case ast.OpIn, ast.OpNotIn:
		if values, ok := value.([]any); ok {
			condition = ast.NewBinaryExpr(
				ast.NewColumn(sb.tableName, column, ""),
				sqlOp,
				ast.NewArray(values),
			)
		} else {
			// Handle single value as array
			condition = ast.NewBinaryExpr(
				ast.NewColumn(sb.tableName, column, ""),
				sqlOp,
				ast.NewArray([]any{value}),
			)
		}
	case ast.OpIsNull, ast.OpIsNotNull:
		condition = ast.NewUnaryExpr(
			ast.NewColumn(sb.tableName, column, ""),
			sqlOp,
			false,
		)
	case ast.OpExists, ast.OpNotExists:
		condition = ast.NewUnaryExpr(
			ast.NewColumn(sb.tableName, column, ""),
			sqlOp,
			true,
		)
	default:
		condition = ast.NewBinaryExpr(
			ast.NewColumn(sb.tableName, column, ""),
			sqlOp,
			ast.NewValue(value),
		)
	}

	sb.stmt.AddWhereCondition(condition, logicalOp)
	return sb
}

// Generic WHERE methods
func (sb *SelectBuilder) Where(column string, operator string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, operator, value, ast.OpAnd)
}

func (sb *SelectBuilder) OrWhere(column string, operator string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, operator, value, ast.OpOr)
}

// AND WHERE methods
func (sb *SelectBuilder) WhereEq(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpEqual, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereNotEq(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotEqual, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereIn(column string, values []any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIn, values, ast.OpAnd)
}

func (sb *SelectBuilder) WhereNotIn(column string, values []any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotIn, values, ast.OpAnd)
}

func (sb *SelectBuilder) WhereLike(column string, pattern string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLike, pattern, ast.OpAnd)
}

func (sb *SelectBuilder) WhereNotLike(column string, pattern string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotLike, pattern, ast.OpAnd)
}

func (sb *SelectBuilder) WhereGt(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpGreaterThan, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereGte(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpGreaterThanOrEqual, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereLt(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLessThan, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereLte(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLessThanOrEqual, value, ast.OpAnd)
}

func (sb *SelectBuilder) WhereIsNull(column string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIsNull, nil, ast.OpAnd)
}

func (sb *SelectBuilder) WhereIsNotNull(column string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIsNotNull, nil, ast.OpAnd)
}

func (sb *SelectBuilder) WhereBetween(column string, start, end any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpBetween, []any{start, end}, ast.OpAnd)
}

func (sb *SelectBuilder) WhereNotBetween(column string, start, end any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotBetween, []any{start, end}, ast.OpAnd)
}

func (sb *SelectBuilder) whereExists(subqueryFn func(*SelectBuilder), logicalOp string) *SelectBuilder {
	subBuilder := NewSelectBuilder("", "", sb.visitor, sb.executor)

	// Link child to parent (zero allocation)
	sb.addChild(subBuilder)

	subqueryFn(subBuilder)

	condition := ast.NewUnaryExpr(
		ast.NewSubqueryExpr(subBuilder.stmt),
		ast.OpExists,
		true,
	)

	sb.stmt.AddWhereCondition(condition, logicalOp)
	return sb
}

func (sb *SelectBuilder) whereNotExists(subqueryFn func(*SelectBuilder), logicalOp string) *SelectBuilder {
	subBuilder := NewSelectBuilder("", "", sb.visitor, sb.executor)

	// Link child to parent (zero allocation)
	sb.addChild(subBuilder)

	subqueryFn(subBuilder)

	condition := ast.NewUnaryExpr(
		ast.NewSubqueryExpr(subBuilder.stmt),
		ast.OpNotExists,
		true,
	)

	sb.stmt.AddWhereCondition(condition, logicalOp)
	return sb
}

func (sb *SelectBuilder) WhereExists(subqueryFn func(*SelectBuilder)) *SelectBuilder {
	return sb.whereExists(subqueryFn, ast.OpAnd)
}

func (sb *SelectBuilder) OrWhereExists(subqueryFn func(*SelectBuilder)) *SelectBuilder {
	return sb.whereExists(subqueryFn, ast.OpOr)
}

func (sb *SelectBuilder) WhereNotExists(subqueryFn func(*SelectBuilder)) *SelectBuilder {
	return sb.whereNotExists(subqueryFn, ast.OpAnd)
}

func (sb *SelectBuilder) OrWhereNotExists(subqueryFn func(*SelectBuilder)) *SelectBuilder {
	return sb.whereNotExists(subqueryFn, ast.OpOr)
}

// OR WHERE methods
func (sb *SelectBuilder) OrWhereEq(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpEqual, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereNotEq(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotEqual, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereIn(column string, values []any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIn, values, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereNotIn(column string, values []any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotIn, values, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereLike(column string, pattern string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLike, pattern, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereNotLike(column string, pattern string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotLike, pattern, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereGt(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpGreaterThan, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereGte(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpGreaterThanOrEqual, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereLt(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLessThan, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereLte(column string, value any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpLessThanOrEqual, value, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereIsNull(column string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIsNull, nil, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereIsNotNull(column string) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpIsNotNull, nil, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereBetween(column string, start, end any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpBetween, []any{start, end}, ast.OpOr)
}

func (sb *SelectBuilder) OrWhereNotBetween(column string, start, end any) *SelectBuilder {
	return sb.whereWithOperator(column, ast.OpNotBetween, []any{start, end}, ast.OpOr)
}

// ORDER BY methods
func (sb *SelectBuilder) OrderByAsc(columns ...string) *SelectBuilder {
	sb.stmt.AddOrderByClause(sb.tableName, false, columns...)
	return sb
}

func (sb *SelectBuilder) OrderByDesc(columns ...string) *SelectBuilder {
	sb.stmt.AddOrderByClause(sb.tableName, true, columns...)
	return sb
}

// LIMIT/OFFSET methods
func (sb *SelectBuilder) Limit(limit int) *SelectBuilder {
	sb.stmt.Limit = ast.NewLimitClause(limit, nil)
	return sb
}

func (sb *SelectBuilder) LimitOffset(limit, offset int) *SelectBuilder {
	sb.stmt.Limit = ast.NewLimitClause(limit, &offset)
	return sb
}

func (sb *SelectBuilder) Offset(offset int) *SelectBuilder {
	if sb.stmt.Limit == nil {
		sb.stmt.Limit = ast.NewLimitClause(0, &offset)
	} else {
		sb.stmt.Limit.Offset = &offset
	}
	return sb
}

// JOIN methods
func (sb *SelectBuilder) InnerJoin(table string) *SelectBuilder {
	sb.stmt.AddJoinClause(ast.JoinInner, "", table, "")
	return sb
}

func (sb *SelectBuilder) On(leftCol, rightCol string) *SelectBuilder {
	// Get the last join clause (the one just added by InnerJoin)
	if len(sb.stmt.Joins) == 0 {
		return sb // No join to add condition to
	}

	lastJoin := sb.stmt.Joins[len(sb.stmt.Joins)-1]

	condition := ast.NewBinaryExpr(
		ast.NewColumn(sb.tableName, leftCol, ""), // Main table column
		ast.OpEqual,
		ast.NewColumn(lastJoin.Table.Name, rightCol, ""), // Joined table column
	)

	// Add condition to the specific join, not to the statement
	if lastJoin.Conditions == nil {
		lastJoin.Conditions = ast.NewJoinCondition()
	}
	lastJoin.Conditions.Append(ast.OpAnd, condition)

	return sb
}

// Build method - using existing visitor pattern
func (sb *SelectBuilder) Build() (string, []interface{}, error) {
	if sb.HasErrors() {
		return "", nil, sb.GetFirstError()
	}

	sql, args, err := sb.visitor.Build(sb.stmt)
	return sql, args, err
}

// Execution methods (new additions for Engine integration)
func (sb *SelectBuilder) Find() ([]map[string]interface{}, error) {
	sql, args, err := sb.Build()
	if err != nil {
		return nil, err
	}

	rows, err := sb.executor.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows)
}

func (sb *SelectBuilder) First() (map[string]interface{}, error) {
	// Temporarily set limit to 1 for efficiency
	originalLimit := sb.stmt.Limit
	sb.stmt.Limit = ast.NewLimitClause(1, nil)

	results, err := sb.Find()

	// Restore original limit
	sb.stmt.Limit = originalLimit

	if err != nil || len(results) == 0 {
		return nil, err
	}
	return results[0], nil
}

func (sb *SelectBuilder) Count() (int64, error) {
	// Store original columns and replace with COUNT(*)
	originalColumns := sb.stmt.Columns
	sb.stmt.Columns = []ast.Node{ast.NewFunction("COUNT", ast.NewValue("*"))}

	sql, args, err := sb.Build()

	// Restore original columns
	sb.stmt.Columns = originalColumns

	if err != nil {
		return 0, err
	}

	var count int64
	err = sb.executor.QueryRow(sql, args...).Scan(&count)
	return count, err
}

func (sb *SelectBuilder) Exists() (bool, error) {
	count, err := sb.Count()
	return count > 0, err
}

// Clone creates a copy of the SelectBuilder for reuse
func (sb *SelectBuilder) Clone() *SelectBuilder {
	newBuilder := NewSelectBuilder(sb.schema, sb.tableName, sb.visitor, sb.executor)
	*newBuilder.stmt = *sb.stmt // Copy the statement
	return newBuilder
}

// Legacy Build method (for backward compatibility)
func (sb *SelectBuilder) BuildString() string {
	sql, _, err := sb.Build()
	if err != nil {
		return ""
	}
	return sql
}

// Helper method for linked list management
func (sb *SelectBuilder) addChild(child *SelectBuilder) {
	child.parent = sb
	child.nextSibling = sb.firstChild
	sb.firstChild = child
}

// Additional SELECT methods for column specification
func (sb *SelectBuilder) Select(columns ...string) *SelectBuilder {
	if len(columns) > 0 {
		sb.stmt.Columns = make([]ast.Node, len(columns))
		for i, col := range columns {
			sb.stmt.Columns[i] = ast.NewColumn(sb.tableName, col, "")
		}
	}
	return sb
}

func (sb *SelectBuilder) Distinct() *SelectBuilder {
	sb.stmt.Distinct = true
	return sb
}

// Aggregate functions
func (sb *SelectBuilder) Sum(column string) *SelectBuilder {
	sb.stmt.Columns = []ast.Node{ast.NewFunction("SUM", ast.NewColumn(sb.tableName, column, ""))}
	return sb
}

func (sb *SelectBuilder) Avg(column string) *SelectBuilder {
	sb.stmt.Columns = []ast.Node{ast.NewFunction("AVG", ast.NewColumn(sb.tableName, column, ""))}
	return sb
}

func (sb *SelectBuilder) Min(column string) *SelectBuilder {
	sb.stmt.Columns = []ast.Node{ast.NewFunction("MIN", ast.NewColumn(sb.tableName, column, ""))}
	return sb
}

func (sb *SelectBuilder) Max(column string) *SelectBuilder {
	sb.stmt.Columns = []ast.Node{ast.NewFunction("MAX", ast.NewColumn(sb.tableName, column, ""))}
	return sb
}
