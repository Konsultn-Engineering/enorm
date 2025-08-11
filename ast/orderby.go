package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type OrderByClause struct {
	Expr       Node
	Desc       bool
	Next       *OrderByClause
	IsGroupEnd bool
}

func NewOrderByClause(expr Node, desc bool) *OrderByClause {
	clause := orderByClausePool.Get().(*OrderByClause)
	clause.Expr = expr
	clause.Desc = desc
	clause.Next = nil
	clause.IsGroupEnd = false
	return clause
}

func (o *OrderByClause) Type() NodeType         { return NodeOrderBy }
func (o *OrderByClause) Accept(v Visitor) error { return v.VisitOrderByClause(o) }
func (o *OrderByClause) Fingerprint() uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("order:"))
	if o.Expr != nil {
		_, _ = h.Write(utils.U64ToBytes(o.Expr.Fingerprint()))
	}
	if o.Desc {
		_, _ = h.Write([]byte("desc"))
	}
	return h.Sum64()
}

func (o *OrderByClause) Release() {
	if o.Expr != nil {
		if releasable, ok := o.Expr.(interface{ Release() }); ok {
			releasable.Release()
		}
	}

	// Release the entire chain
	if o.Next != nil {
		o.Next.Release()
	}

	o.Expr = nil
	o.Desc = false
	o.Next = nil
	orderByClausePool.Put(o)
}
