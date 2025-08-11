package ast

import "github.com/Konsultn-Engineering/enorm/utils"

type WhereCondition struct {
	Condition Node
	Operator  string
	Next      *WhereCondition
}

type WhereClause struct {
	First *WhereCondition // Head of the linked list
	Tail  *WhereCondition // Tail for O(1) append operations
}

func NewWhereClause(condition Node, operator string) *WhereCondition {
	wc := whereConditionPool.Get().(*WhereCondition)
	wc.Condition = condition
	wc.Operator = operator
	wc.Next = nil
	return wc
}

func (w *WhereClause) Type() NodeType         { return NodeWhere }
func (w *WhereClause) Accept(v Visitor) error { return v.VisitWhereClause(w) }
func (w *WhereClause) Fingerprint() uint64 {
	if w.First == nil {
		return 0
	}

	hash := uint64(0)
	cond := w.First
	for cond != nil {
		if cond.Condition != nil {
			hash ^= cond.Condition.Fingerprint()
		}
		// Include operator in fingerprint
		hash ^= utils.U64(cond.Operator)
		cond = cond.Next
	}
	return hash
}

func (wc *WhereCondition) Release() {
	if wc.Condition != nil {
		if releasable, ok := wc.Condition.(interface{ Release() }); ok {
			releasable.Release()
		}
	}

	// Release entire chain
	if wc.Next != nil {
		wc.Next.Release()
	}

	wc.Condition = nil
	wc.Operator = ""
	wc.Next = nil
	whereConditionPool.Put(wc)
}

func (w *WhereClause) Release() {
	if w.First != nil {
		w.First.Release()
	}
	w.First = nil
	w.Tail = nil
}
