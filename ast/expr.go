package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type BinaryExpr struct {
	Left     Node
	Operator string
	Right    Node
}

func NewBinaryExpr(left Node, op string, right Node) *BinaryExpr {
	b := binaryExprPool.Get().(*BinaryExpr)
	b.Left = left
	b.Operator = op
	b.Right = right
	return b
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

func (b *BinaryExpr) Release() {
	if releasable, ok := b.Left.(interface{ Release() }); ok {
		releasable.Release()
	}
	if releasable, ok := b.Right.(interface{ Release() }); ok {
		releasable.Release()
	}
	binaryExprPool.Put(b)
}

type UnaryExpr struct {
	Operator string
	Operand  Node
	IsPrefix bool // Add this field
}

func NewUnaryExpr(left Node, op string, isPrefix bool) *UnaryExpr {
	b := unaryExprPool.Get().(*UnaryExpr)
	b.Operand = left
	b.Operator = op
	b.IsPrefix = isPrefix // Set the prefix flag
	return b
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
