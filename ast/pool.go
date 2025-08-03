package ast

import (
	"sync"
)

// Enhanced pools with better sizing and additional node types
var (
	selectStmtPool = sync.Pool{
		New: func() any {
			return &SelectStmt{
				Columns: make([]Node, 0, 10),
				Joins:   make([]*JoinClause, 0, 4),
				OrderBy: make([]*OrderByClause, 0, 4),
			}
		},
	}

	columnPool = sync.Pool{
		New: func() any { return &Column{} },
	}

	tablePool = sync.Pool{
		New: func() any { return &Table{} },
	}

	valuePool = sync.Pool{
		New: func() any { return &Value{} },
	}

	binaryExprPool = sync.Pool{
		New: func() any { return &BinaryExpr{} },
	}

	whereClausePool = sync.Pool{
		New: func() any { return &WhereClause{} },
	}

	limitClausePool = sync.Pool{
		New: func() any { return &LimitClause{} },
	}

	orderByClausePool = sync.Pool{
		New: func() any { return &OrderByClause{} },
	}

	arrayPool = sync.Pool{
		New: func() any {
			return &Array{Values: make([]Value, 0, 8)}
		},
	}

	groupByClausePool = sync.Pool{
		New: func() any {
			return &GroupByClause{Exprs: make([]Node, 0, 4)}
		},
	}

	joinClausePool = sync.Pool{
		New: func() any { return &JoinClause{} },
	}
)

// NewSelectStmt creates a new SelectStmt with preallocated slices for performance
func NewSelectStmt() *SelectStmt {
	s := selectStmtPool.Get().(*SelectStmt)
	s.Columns = s.Columns[:0]
	s.From = nil
	s.Joins = s.Joins[:0]
	s.Where = nil
	s.GroupBy = nil
	s.Having = nil
	s.OrderBy = s.OrderBy[:0]
	s.Limit = nil
	s.ForUpdate = false
	return s
}

func (s *SelectStmt) Release() {
	for _, col := range s.Columns {
		if releasable, ok := col.(interface{ Release() }); ok {
			releasable.Release()
		}
	}
	if s.From != nil {
		s.From.Release()
	}
	for _, join := range s.Joins {
		join.Release()
	}
	if s.Where != nil {
		s.Where.Release()
	}
	if s.GroupBy != nil {
		s.GroupBy.Release()
	}
	for _, order := range s.OrderBy {
		order.Release()
	}
	if s.Limit != nil {
		s.Limit.Release()
	}
	selectStmtPool.Put(s)
}

func NewColumn(name string) *Column {
	c := columnPool.Get().(*Column)
	c.Table = ""
	c.Name = name
	c.Alias = ""
	return c
}

func (c *Column) Release() {
	columnPool.Put(c)
}

func NewTable(name string) *Table {
	t := tablePool.Get().(*Table)
	t.Schema = ""
	t.Name = name
	t.Alias = ""
	return t
}

func (t *Table) Release() {
	tablePool.Put(t)
}

func NewValue(val any) *Value {
	v := valuePool.Get().(*Value)
	v.Val = val
	return v
}

func (v *Value) Release() {
	valuePool.Put(v)
}

func NewBinaryExpr(left Node, op string, right Node) *BinaryExpr {
	b := binaryExprPool.Get().(*BinaryExpr)
	b.Left = left
	b.Operator = op
	b.Right = right
	return b
}

func (b *BinaryExpr) Release() {
	if releasable, ok := b.Left.(interface{ Release() }); ok {
		releasable.Release()
	}
	if releasable, ok := b.Right.(interface{ Release() }); ok {
		releasable.Release()
	}
	binaryExprPool.Put(b)
}

func NewWhereClause(condition Node) *WhereClause {
	w := whereClausePool.Get().(*WhereClause)
	w.Condition = condition
	return w
}

func (w *WhereClause) Release() {
	if releasable, ok := w.Condition.(interface{ Release() }); ok {
		releasable.Release()
	}
	whereClausePool.Put(w)
}

func NewLimitClause(count, offset *int) *LimitClause {
	l := limitClausePool.Get().(*LimitClause)
	l.Count = count
	l.Offset = offset
	return l
}

func (l *LimitClause) Release() {
	limitClausePool.Put(l)
}

func NewOrderByClause(expr Node, desc bool) *OrderByClause {
	o := orderByClausePool.Get().(*OrderByClause)
	o.Expr = expr
	o.Desc = desc
	return o
}

func (o *OrderByClause) Release() {
	if releasable, ok := o.Expr.(interface{ Release() }); ok {
		releasable.Release()
	}
	orderByClausePool.Put(o)
}

func NewArray(values []any) *Array {
	a := arrayPool.Get().(*Array)
	a.Values = a.Values[:0]

	for _, val := range values {
		a.Values = append(a.Values, Value{Val: val})
	}
	return a
}

func (a *Array) Release() {
	a.Values = a.Values[:0]
	arrayPool.Put(a)
}

func NewGroupByClause(exprs []Node) *GroupByClause {
	g := groupByClausePool.Get().(*GroupByClause)
	g.Exprs = g.Exprs[:0]
	g.Exprs = append(g.Exprs, exprs...)
	return g
}

func (g *GroupByClause) Release() {
	for _, expr := range g.Exprs {
		if releasable, ok := expr.(interface{ Release() }); ok {
			releasable.Release()
		}
	}
	g.Exprs = g.Exprs[:0]
	groupByClausePool.Put(g)
}

func NewJoinClause(joinType JoinType, table *Table, condition Node) *JoinClause {
	j := joinClausePool.Get().(*JoinClause)
	j.JoinType = joinType
	j.Table = table
	j.Condition = condition
	return j
}

func (j *JoinClause) Release() {
	if j.Table != nil {
		j.Table.Release()
	}
	if releasable, ok := j.Condition.(interface{ Release() }); ok {
		releasable.Release()
	}
	joinClausePool.Put(j)
}
