package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
	"reflect"
	"strconv"
)

type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinCross
)

// ----- Join conditions: singly-linked list with rolling fingerprint -----

type JoinConditionNode struct {
	Condition Node
	Operator  string
	Next      *JoinConditionNode

	fp  uint64 // node-only
	acc uint64 // cumulative (chain fp up to this node)
}

type JoinCondition struct {
	First *JoinConditionNode
	Tail  *JoinConditionNode
}

func (c *JoinCondition) Append(op string, cond Node) *JoinConditionNode {
	n := joinConditionNodePool.Get().(*JoinConditionNode)
	n.Operator, n.Condition, n.Next = op, cond, nil

	// node-only fp = hash(op) mixed with hash(type(cond))
	opFP := utils.U64(op)
	var tyFP uint64
	if cond != nil {
		tyFP = utils.U64(reflect.TypeOf(cond).String())
	}
	n.fp = utils.Mix64(opFP, tyFP)

	if c.First == nil {
		n.acc = utils.Mix64(0x9e3779b185ebca87, n.fp) // seeded start
		c.First, c.Tail = n, n
		return n
	}
	n.acc = utils.Mix64(c.Tail.acc, n.fp)
	c.Tail.Next = n
	c.Tail = n
	return n
}

func (c *JoinCondition) Fingerprint() uint64 {
	if c == nil || c.Tail == nil {
		return 0
	}
	return c.Tail.acc
}

func (c *JoinCondition) Release() {
	if c == nil {
		return
	}
	for cur := c.First; cur != nil; {
		next := cur.Next
		cur.Release()
		cur = next
	}
	c.First, c.Tail = nil, nil
	joinConditionPool.Put(c)
}

func (n *JoinConditionNode) Release() {
	if n == nil {
		return
	}
	n.Condition = nil
	n.Operator = ""
	n.Next = nil
	n.fp, n.acc = 0, 0
	joinConditionNodePool.Put(n)
}

// ----- JoinClause -----

type JoinClause struct {
	JoinType   JoinType
	Table      *Table
	Conditions *JoinCondition
}

func NewJoinClause(joinType JoinType, schema, name, alias string) *JoinClause {
	j := joinClausePool.Get().(*JoinClause)
	j.JoinType = joinType
	j.Table = NewTable(schema, name, alias)
	j.Conditions = nil
	return j
}

func (j *JoinClause) Type() NodeType         { return NodeJoin }
func (j *JoinClause) Accept(v Visitor) error { return v.VisitJoinClause(j) }

func (j *JoinClause) Fingerprint() uint64 {
	// comprehensive per-clause fp: kind + join type + table fp + conditions fp
	h := fnv.New64a()
	h.Write([]byte("join:" + strconv.Itoa(int(j.Type())) + ":" + strconv.Itoa(int(j.JoinType))))
	fp := h.Sum64()

	if j.Table != nil {
		fp = utils.Mix64(fp, j.Table.Fingerprint())
	}
	if j.Conditions != nil {
		fp = utils.Mix64(fp, j.Conditions.Fingerprint())
	}
	return fp
}

func (j *JoinClause) Release() {
	if j == nil {
		return
	}
	if j.Table != nil {
		j.Table.Release()
		j.Table = nil
	}
	if j.Conditions != nil {
		j.Conditions.Release()
		j.Conditions = nil
	}
	j.JoinType = 0
	joinClausePool.Put(j)
}
