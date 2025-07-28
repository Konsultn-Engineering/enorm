package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type OrderByClause struct {
	Expr Node
	Desc bool
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
