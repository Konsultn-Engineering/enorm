package ast

type InsertStmt struct {
	Table   *Table
	Columns []string
	Values  [][]Node
}

func (i *InsertStmt) Type() NodeType         { return NodeInsert }
func (i *InsertStmt) Accept(v Visitor) error { return v.VisitInsert(i) }
