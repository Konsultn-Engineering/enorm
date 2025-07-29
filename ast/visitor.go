package ast

type Visitor interface {
	VisitSelect(*SelectStmt) error
	VisitInsert(*InsertStmt) error
	VisitUpdate(*UpdateStmt) error
	VisitDelete(*DeleteStmt) error
	VisitCreateTable(*CreateTableStmt) error

	VisitColumn(*Column) error
	VisitTable(*Table) error
	VisitValue(*Value) error
	VisitArray(*Array) error
	VisitFunction(*Function) error
	VisitGroupedExpr(*GroupedExpr) error
	VisitBinaryExpr(*BinaryExpr) error
	VisitUnaryExpr(*UnaryExpr) error
	VisitSubqueryExpr(*SubqueryExpr) error

	VisitWhereClause(*WhereClause) error
	VisitJoinClause(*JoinClause) error
	VisitGroupBy(*GroupByClause) error
	VisitOrderByClause(*OrderByClause) error
	VisitLimitClause(*LimitClause) error
	Build(root Node) (string, []any, error)
	Release()
}
