package ast

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
