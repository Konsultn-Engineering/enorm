package ast

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
	"strconv"
)

type Column struct {
	Table string
	Name  string
	Alias string
}

func (c *Column) Type() NodeType         { return NodeColumn }
func (c *Column) Accept(v Visitor) error { return v.VisitColumn(c) }
func (c *Column) Fingerprint() uint64 {
	s := c.Table + "." + c.Name
	return utils.FingerprintString(s)
}

type Table struct {
	Schema string
	Name   string
	Alias  string
}

func (t *Table) Type() NodeType         { return NodeTable }
func (t *Table) Accept(v Visitor) error { return v.VisitTable(t) }
func (t *Table) Fingerprint() uint64 {
	s := t.Schema + "." + t.Name + "." + t.Alias
	return utils.FingerprintString(s)
}

type Value struct {
	Val       interface{}
	ValueType ValueType
}

func (v *Value) Type() NodeType           { return NodeValue }
func (v *Value) Accept(vis Visitor) error { return vis.VisitValue(v) }
func (v *Value) Fingerprint() uint64 {
	s := "val:" + strconv.FormatInt(int64(v.Type()), 10) + ":" + fmt.Sprint(v.Val)
	return utils.FingerprintString(s)
}

type ValueType int

const (
	ValueNull ValueType = iota
	ValueBool
	ValueInt
	ValueFloat
	ValueString
	ValueTime
)

type Function struct {
	Name string
	Args []Node
}

func (f *Function) Type() NodeType         { return NodeFunction }
func (f *Function) Accept(v Visitor) error { return v.VisitFunction(f) }
func (f *Function) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("func:" + f.Name))
	for _, arg := range f.Args {
		if fp, ok := arg.(Node); ok {
			h.Write(utils.U64ToBytes(fp.Fingerprint()))
		}
	}
	return h.Sum64()
}

type GroupedExpr struct {
	Expr Node
}

func (g *GroupedExpr) Type() NodeType {
	return NodeGroupedExpr
}

func (g *GroupedExpr) Accept(v Visitor) error {
	return v.VisitGroupedExpr(g)
}

func (g *GroupedExpr) Fingerprint() uint64 {
	if g.Expr == nil {
		return 0
	}
	return g.Expr.Fingerprint()
}

type BinaryExpr struct {
	Left     Node
	Operator string
	Right    Node
}

func (b *BinaryExpr) Type() NodeType         { return NodeBinaryExpr }
func (b *BinaryExpr) Accept(v Visitor) error { return v.VisitBinaryExpr(b) }
func (b *BinaryExpr) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("bin:" + b.Operator))
	if l, ok := b.Left.(Node); ok {
		h.Write(utils.U64ToBytes(l.Fingerprint()))
	}
	if r, ok := b.Right.(Node); ok {
		h.Write(utils.U64ToBytes(r.Fingerprint()))
	}
	return h.Sum64()
}

type UnaryExpr struct {
	Operator string
	Operand  Node
}

func (u *UnaryExpr) Type() NodeType         { return NodeUnaryExpr }
func (u *UnaryExpr) Accept(v Visitor) error { return v.VisitUnaryExpr(u) }
func (u *UnaryExpr) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("unary:" + u.Operator))
	if op, ok := u.Operand.(Node); ok {
		h.Write(utils.U64ToBytes(op.Fingerprint()))
	}
	return h.Sum64()
}
