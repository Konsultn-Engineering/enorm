package ast

import (
	"encoding/binary"
	"hash/fnv"
)

type SubqueryExpr struct {
	Stmt Node
}

func NewSubqueryExpr(stmt Node) *SubqueryExpr {
	s := subqueryExprPool.Get().(*SubqueryExpr)
	s.Stmt = stmt
	return s
}

func (s *SubqueryExpr) Type() NodeType {
	return NodeSubqueryExpr
}

func (s *SubqueryExpr) Accept(v Visitor) error {
	return v.VisitSubqueryExpr(s)
}

func (s *SubqueryExpr) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("SubqueryExpr"))
	if s.Stmt != nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, s.Stmt.Fingerprint())
		h.Write(b)
	}
	return h.Sum64()
}
