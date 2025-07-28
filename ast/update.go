package ast

type UpdateStmt struct {
	Table *Table
	Set   map[string]Node
	Where *WhereClause
}

func (u *UpdateStmt) Type() NodeType         { return NodeUpdate }
func (u *UpdateStmt) Accept(v Visitor) error { return v.VisitUpdate(u) }
