package ast

import (
	"hash/fnv"
	"strconv"
)

type LimitClause struct {
	Count  int
	Offset *int
}

func NewLimitClause(count int, offset *int) *LimitClause {
	l := limitClausePool.Get().(*LimitClause)
	l.Count = count
	l.Offset = offset
	return l
}

func (l *LimitClause) Type() NodeType         { return NodeLimit }
func (l *LimitClause) Accept(v Visitor) error { return v.VisitLimitClause(l) }
func (l *LimitClause) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("limit:"))
	h.Write([]byte(strconv.Itoa(l.Count)))
	h.Write([]byte(":"))
	if l.Offset != nil {
		h.Write([]byte(strconv.Itoa(*l.Offset)))
	}
	return h.Sum64()
}

func (l *LimitClause) Release() {
	limitClausePool.Put(l)
}
