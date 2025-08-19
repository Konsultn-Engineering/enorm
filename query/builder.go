package query

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"sync"
)

var (
	builderPool = sync.Pool{
		New: func() any {
			return &Builder{
				stmt: ast.NewSelectStmt(),
			}
		},
	}
)

type Builder struct {
	tableName   string
	visitor     *visitor.SQLVisitor
	errors      []error
	stmt        *ast.SelectStmt
	schema      string
	paramCount  int
	parent      *Builder // Points to parent builder
	firstChild  *Builder // Head of children linked list
	nextSibling *Builder // Next child in parent's list
}

// NewBuilder creates a new Builder instance
func NewBuilder(schema string, table string, visitor *visitor.SQLVisitor) *Builder {
	builder := builderPool.Get().(*Builder)

	// Initialize fields
	builder.tableName = table
	builder.visitor = visitor
	builder.errors = make([]error, 0)

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
func (b *Builder) Release() {
	// Release all children first (walk linked list)
	child := b.firstChild
	for child != nil {
		next := child.nextSibling
		child.Release()
		child = next
	}

	// Clear all links
	b.firstChild = nil
	b.parent = nil
	b.nextSibling = nil

	// Release self
	if b.stmt != nil {
		b.stmt.Release()
	}
	b.schema = ""
	b.paramCount = 0
	b.errors = nil
	builderPool.Put(b)
}

// Core accessors
func (b *Builder) TableName() string {
	return b.tableName
}

func (b *Builder) Visitor() *visitor.SQLVisitor {
	return b.visitor
}

func (b *Builder) GetStatement() *ast.SelectStmt {
	return b.stmt
}

// Error handling
func (b *Builder) AddError(err error) {
	if err != nil {
		b.errors = append(b.errors, err)
	}
}

func (b *Builder) HasErrors() bool {
	return len(b.errors) > 0
}

func (b *Builder) GetErrors() []error {
	return b.errors
}

func (b *Builder) GetFirstError() error {
	if len(b.errors) > 0 {
		return b.errors[0]
	}
	return nil
}

// Core WHERE method - this is the foundation for all WHERE operations
func (b *Builder) Where(column string, operator string, value any) *Builder {
	return b.whereWithOperator(column, operator, value, ast.OpAnd)
}

func (b *Builder) OrWhere(column string, operator string, value any) *Builder {
	return b.whereWithOperator(column, operator, value, ast.OpOr)
}

// Core ORDER BY method
func (b *Builder) OrderBy(columns []string, desc bool) *Builder {
	b.stmt.AddOrderByClause(b.tableName, desc, columns...)
	return b
}

// Core LIMIT/OFFSET methods
func (b *Builder) Limit(limit int) *Builder {
	b.stmt.Limit = ast.NewLimitClause(limit, nil)
	return b
}

func (b *Builder) Offset(offset int) *Builder {
	if b.stmt.Limit == nil {
		b.stmt.Limit = ast.NewLimitClause(0, &offset)
	} else {
		b.stmt.Limit.Offset = &offset
	}
	return b
}

// Core SELECT methods
func (b *Builder) Select(columns []string) *Builder {
	if len(columns) == 0 {
		return b
	}

	if b.stmt.Columns == nil {
		b.stmt.Columns = make([]ast.Node, 0, len(columns))
	}

	for _, col := range columns {
		b.stmt.Columns = append(b.stmt.Columns, ast.NewColumn(b.tableName, col, ""))
	}

	return b
}

func (b *Builder) SelectRaw(expr string, alias string) *Builder {
	column := ast.NewColumn("", expr, alias)

	if b.stmt.Columns == nil {
		b.stmt.Columns = make([]ast.Node, 0, 1)
	}

	b.stmt.Columns = append(b.stmt.Columns, column)
	return b
}

func (b *Builder) Distinct() *Builder {
	b.stmt.Distinct = true
	return b
}

// Core JOIN methods
func (b *Builder) Join(joinType ast.JoinType, table string, leftCol string, operator string, rightCol string) *Builder {
	b.stmt.AddJoinClause(joinType, "", table, "")

	if len(b.stmt.Joins) > 0 {
		lastJoin := b.stmt.Joins[len(b.stmt.Joins)-1]

		condition := ast.NewBinaryExpr(
			ast.NewColumn(b.tableName, leftCol, ""),
			operator,
			ast.NewColumn(lastJoin.Table.Name, rightCol, ""),
		)

		if lastJoin.Conditions == nil {
			lastJoin.Conditions = ast.NewJoinCondition()
		}
		lastJoin.Conditions.Append(ast.OpAnd, condition)
	}

	return b
}

// Core subquery methods
func (b *Builder) WhereSubquery(column string, operator string, subqueryFn func(*Builder)) *Builder {
	subBuilder := NewBuilder("", "", b.visitor)
	b.addChild(subBuilder)
	subqueryFn(subBuilder)

	condition := ast.NewBinaryExpr(
		ast.NewColumn(b.tableName, column, ""),
		operator,
		ast.NewSubqueryExpr(subBuilder.stmt),
	)

	b.stmt.AddWhereCondition(condition, ast.OpAnd)
	return b
}

func (b *Builder) WhereExists(subqueryFn func(*Builder)) *Builder {
	subBuilder := NewBuilder("", "", b.visitor)
	b.addChild(subBuilder)
	subqueryFn(subBuilder)

	condition := ast.NewUnaryExpr(
		ast.NewSubqueryExpr(subBuilder.stmt),
		ast.OpExists,
		true,
	)

	b.stmt.AddWhereCondition(condition, ast.OpAnd)
	return b
}

// Build method - using existing visitor pattern
func (b *Builder) Build() (string, []interface{}, error) {
	if b.HasErrors() {
		return "", nil, b.GetFirstError()
	}

	sql, args, err := b.visitor.Build(b.stmt)
	return sql, args, err
}

// Clone creates a copy of the Builder for reuse
func (b *Builder) Clone() *Builder {
	newBuilder := NewBuilder(b.schema, b.tableName, b.visitor)
	*newBuilder.stmt = *b.stmt // Copy the statement
	return newBuilder
}

// Private helper methods
func (b *Builder) whereWithOperator(column string, sqlOp string, value any, logicalOp string) *Builder {
	var condition ast.Node

	switch sqlOp {
	case ast.OpIn, ast.OpNotIn:
		if values, ok := value.([]any); ok {
			condition = ast.NewBinaryExpr(
				ast.NewColumn(b.tableName, column, ""),
				sqlOp,
				ast.NewArray(values),
			)
		} else {
			// Handle single value as array
			condition = ast.NewBinaryExpr(
				ast.NewColumn(b.tableName, column, ""),
				sqlOp,
				ast.NewArray([]any{value}),
			)
		}
	case ast.OpIsNull, ast.OpIsNotNull:
		condition = ast.NewUnaryExpr(
			ast.NewColumn(b.tableName, column, ""),
			sqlOp,
			false,
		)
	case ast.OpExists, ast.OpNotExists:
		condition = ast.NewUnaryExpr(
			ast.NewColumn(b.tableName, column, ""),
			sqlOp,
			true,
		)
	default:
		condition = ast.NewBinaryExpr(
			ast.NewColumn(b.tableName, column, ""),
			sqlOp,
			ast.NewValue(value),
		)
	}

	b.stmt.AddWhereCondition(condition, logicalOp)
	return b
}

func (b *Builder) addChild(child *Builder) {
	child.parent = b
	child.nextSibling = b.firstChild
	b.firstChild = child
}
