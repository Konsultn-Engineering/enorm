package ast

import (
	"sync"
)

// Pools for commonly used AST nodes
var (
	selectStmtPool = sync.Pool{
		New: func() any {
			return &SelectStmt{
				Columns: make([]Node, 0, 10), // Larger initial capacity
				Joins:   make([]*JoinClause, 0, 4),
				OrderBy: make([]*OrderByClause, 0, 4),
			}
		},
	}
	
	columnPool = sync.Pool{
		New: func() any {
			return &Column{}
		},
	}
	
	tablePool = sync.Pool{
		New: func() any {
			return &Table{}
		},
	}
	
	valuePool = sync.Pool{
		New: func() any {
			return &Value{}
		},
	}
	
	binaryExprPool = sync.Pool{
		New: func() any {
			return &BinaryExpr{}
		},
	}
	
	whereClausePool = sync.Pool{
		New: func() any {
			return &WhereClause{}
		},
	}
	
	limitClausePool = sync.Pool{
		New: func() any {
			return &LimitClause{}
		},
	}
)

// Factory functions for pooled AST nodes
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
	// Release nested nodes first
	for _, col := range s.Columns {
		if c, ok := col.(*Column); ok {
			c.Release()
		}
	}
	if s.From != nil {
		s.From.Release()
	}
	if s.Where != nil {
		s.Where.Release()
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
	if c, ok := b.Left.(*Column); ok {
		c.Release()
	}
	if v, ok := b.Right.(*Value); ok {
		v.Release()
	}
	binaryExprPool.Put(b)
}

func NewWhereClause(condition Node) *WhereClause {
	w := whereClausePool.Get().(*WhereClause)
	w.Condition = condition
	return w
}

func (w *WhereClause) Release() {
	if b, ok := w.Condition.(*BinaryExpr); ok {
		b.Release()
	}
	whereClausePool.Put(w)
}

func NewLimitClause(count *int) *LimitClause {
	l := limitClausePool.Get().(*LimitClause)
	l.Count = count
	l.Offset = nil
	return l
}

func (l *LimitClause) Release() {
	limitClausePool.Put(l)
}