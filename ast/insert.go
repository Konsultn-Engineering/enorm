package ast

type InsertStmt struct {
	Table     *Table
	Columns   []string
	Values    [][]Node
	Returning []string
}

func NewInsertStmt(table *Table, columns []string, values [][]Node, returning []string) *InsertStmt {
	return &InsertStmt{
		Table:     table,
		Columns:   columns,
		Values:    values,
		Returning: returning,
	}
}

func (i *InsertStmt) Type() NodeType         { return NodeInsert }
func (i *InsertStmt) Accept(v Visitor) error { return v.VisitInsert(i) }
func (i *InsertStmt) Fingerprint() uint64 {
	return 0000000000000000
}
