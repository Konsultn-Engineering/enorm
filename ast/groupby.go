package ast

import (
	"encoding/binary"
	"hash/fnv"
)

type GroupByClause struct {
	Exprs []Node
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
