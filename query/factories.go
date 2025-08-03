package query

import "github.com/Konsultn-Engineering/enorm/ast"

// Simple, direct factories - no complex registry needed
func Column(name string) *ast.Column {
	return ast.NewColumn(name) // Uses your existing pool
}

func Columns(names ...string) []ast.Node {
	nodes := make([]ast.Node, len(names))
	for i, name := range names {
		nodes[i] = ast.NewColumn(name)
	}
	return nodes
}

func AllColumns() *ast.Column {
	return ast.NewColumn("*")
}

func WhereEq(column string, value any) *ast.BinaryExpr {
	return ast.NewBinaryExpr(
		ast.NewColumn(column),
		"=",
		ast.NewValue(value),
	)
}

func WhereIn(column string, values []any) *ast.BinaryExpr {
	arrayValues := make([]ast.Value, len(values))
	for i, v := range values {
		arrayValues[i] = ast.Value{Val: v}
	}

	return ast.NewBinaryExpr(
		ast.NewColumn(column),
		"IN",
		&ast.Array{Values: arrayValues},
	)
}

func OrderBy(column string, desc bool) *ast.OrderByClause {
	return &ast.OrderByClause{
		Expr: ast.NewColumn(column),
		Desc: desc,
	}
}
