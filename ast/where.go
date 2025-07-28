package ast

type WhereClause struct {
	Condition Node
}

func (w *WhereClause) Type() NodeType         { return NodeWhere }
func (w *WhereClause) Accept(v Visitor) error { return v.VisitWhereClause(w) }
func (w *WhereClause) Fingerprint() uint64 {
	if w.Condition == nil {
		return 0
	}
	return w.Condition.Fingerprint()
}
