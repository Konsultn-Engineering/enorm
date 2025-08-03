package ast

// High-level factory functions that leverage pooling
func Columns(names ...string) []Node {
	nodes := make([]Node, len(names))
	for i, name := range names {
		nodes[i] = NewColumn(name)
	}
	return nodes
}

func AllColumns() []Node {
	return []Node{NewColumn("*")}
}

func WhereEq(column string, value any) *WhereClause {
	return NewWhereClause(
		NewBinaryExpr(
			NewColumn(column),
			"=",
			NewValue(value),
		),
	)
}

func WhereIn(column string, values []any) *WhereClause {
	return NewWhereClause(
		NewBinaryExpr(
			NewColumn(column),
			"IN",
			NewArray(values),
		),
	)
}

func WhereLike(column string, pattern string) *WhereClause {
	return NewWhereClause(
		NewBinaryExpr(
			NewColumn(column),
			"LIKE",
			NewValue(pattern),
		),
	)
}

func WhereAnd(left, right Node) *WhereClause {
	return NewWhereClause(
		NewBinaryExpr(left, "AND", right),
	)
}

func WhereOr(left, right Node) *WhereClause {
	return NewWhereClause(
		NewBinaryExpr(left, "OR", right),
	)
}

func OrderBy(column string, desc bool) *OrderByClause {
	return NewOrderByClause(NewColumn(column), desc)
}

func OrderByAsc(column string) *OrderByClause {
	return OrderBy(column, false)
}

func OrderByDesc(column string) *OrderByClause {
	return OrderBy(column, true)
}

func Limit(count int) *LimitClause {
	c := count
	return NewLimitClause(&c, nil)
}

func LimitOffset(count, offset int) *LimitClause {
	c, o := count, offset
	return NewLimitClause(&c, &o)
}

func InnerJoin(table string, condition Node) *JoinClause {
	return NewJoinClause(JoinInner, NewTable(table), condition)
}

func LeftJoin(table string, condition Node) *JoinClause {
	return NewJoinClause(JoinLeft, NewTable(table), condition)
}

func RightJoin(table string, condition Node) *JoinClause {
	return NewJoinClause(JoinRight, NewTable(table), condition)
}

func FullJoin(table string, condition Node) *JoinClause {
	return NewJoinClause(JoinFull, NewTable(table), condition)
}

func CrossJoin(table string, condition Node) *JoinClause {
	return NewJoinClause(JoinCross, NewTable(table), condition)
}

func JoinOn(leftTable, leftColumn, rightTable, rightColumn string) Node {
	return NewBinaryExpr(
		NewColumn(leftTable+"."+leftColumn),
		"=",
		NewColumn(rightTable+"."+rightColumn),
	)
}
