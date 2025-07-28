package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
	"strconv"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

type JoinClause struct {
	JoinType  JoinType
	Table     *Table
	Condition Node
}

func (j *JoinClause) Type() NodeType         { return NodeJoin }
func (j *JoinClause) Accept(v Visitor) error { return v.VisitJoinClause(j) }
func (j *JoinClause) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("join:" + strconv.Itoa(int(j.Type()))))
	if j.Table != nil {
		h.Write(utils.U64ToBytes(j.Table.Fingerprint()))
	}
	if j.Condition != nil {
		h.Write(utils.U64ToBytes(j.Condition.Fingerprint()))
	}
	return h.Sum64()
}
