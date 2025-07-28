package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type SelectStmt struct {
	Columns   []Node
	From      *Table
	Joins     []*JoinClause
	Where     *WhereClause
	GroupBy   *GroupByClause // updated
	Having    *WhereClause
	OrderBy   []*OrderByClause
	Limit     *LimitClause
	ForUpdate bool
}

func (s *SelectStmt) Type() NodeType         { return NodeSelect }
func (s *SelectStmt) Accept(v Visitor) error { return v.VisitSelect(s) }
func (s *SelectStmt) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("select:"))
	if s.From != nil {
		h.Write(utils.U64ToBytes(s.From.Fingerprint()))
	}
	for _, col := range s.Columns {
		h.Write(utils.U64ToBytes(col.Fingerprint()))
	}
	for _, j := range s.Joins {
		h.Write(utils.U64ToBytes(j.Fingerprint()))
	}
	if s.Where != nil {
		h.Write(utils.U64ToBytes(s.Where.Fingerprint()))
	}
	if s.GroupBy != nil {
		h.Write(utils.U64ToBytes(s.GroupBy.Fingerprint()))
	}
	if s.Having != nil {
		h.Write(utils.U64ToBytes(s.Having.Fingerprint()))
	}
	for _, o := range s.OrderBy {
		h.Write(utils.U64ToBytes(o.Fingerprint()))
	}
	if s.Limit != nil {
		h.Write(utils.U64ToBytes(s.Limit.Fingerprint()))
	}
	if s.ForUpdate {
		h.Write([]byte("for_update"))
	}
	return h.Sum64()
}
