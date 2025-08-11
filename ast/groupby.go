package ast

import (
	"encoding/binary"
	"hash/fnv"
)

type GroupByClause struct {
	Exprs []Node
}

func NewGroupByClause(exprs []Node) *GroupByClause {
	g := groupByClausePool.Get().(*GroupByClause)
	g.Exprs = g.Exprs[:0]
	g.Exprs = append(g.Exprs, exprs...)
	return g
}

func (g *GroupByClause) Type() NodeType         { return NodeGroupBy }
func (g *GroupByClause) Accept(v Visitor) error { return v.VisitGroupBy(g) }
func (g *GroupByClause) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("groupby:"))
	for _, expr := range g.Exprs {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, expr.Fingerprint())
		h.Write(b)
	}
	return h.Sum64()
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
