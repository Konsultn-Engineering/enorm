package ast

type DeleteStmt struct {
	Table *Table
	Where *WhereClause
}

func (d *DeleteStmt) Type() NodeType         { return NodeDelete }
func (d *DeleteStmt) Accept(v Visitor) error { return v.VisitDelete(d) }
