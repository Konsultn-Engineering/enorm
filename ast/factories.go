package ast

// Optimized factory functions that use pooled slices
//func Columns(names ...string) []Node {
//	if len(names) == 0 {
//		return nil
//	}
//
//	nodes := getNodeSlice(len(names))
//	for _, name := range names {
//		nodes = append(nodes, NewColumn(name))
//	}
//	return nodes
//}

func AllColumns(table string) []Node {
	nodes := getNodeSlice(1)
	nodes = append(nodes, NewColumn(table, "*", ""))
	return nodes
}

func Limit(count int) *LimitClause {
	c := count
	return NewLimitClause(c, nil)
}

func LimitOffset(count, offset int) *LimitClause {
	c, o := count, offset
	return NewLimitClause(c, &o)
}

//
//func InnerJoin(table string, condition Node) *JoinClause {
//	return NewJoinClause(JoinInner, NewTable(table), condition)
//}
//
//func LeftJoin(table string, condition Node) *JoinClause {
//	return NewJoinClause(JoinLeft, NewTable(table), condition)
//}
//
//func RightJoin(table string, condition Node) *JoinClause {
//	return NewJoinClause(JoinRight, NewTable(table), condition)
//}
//
//func FullJoin(table string, condition Node) *JoinClause {
//	return NewJoinClause(JoinFull, NewTable(table), condition)
//}
//
//func CrossJoin(table string, condition Node) *JoinClause {
//	return NewJoinClause(JoinCross, NewTable(table), condition)
//}

//func JoinOn(leftTable, leftColumn, rightTable, rightColumn string) Node {
//	return NewBinaryExpr(
//		NewColumn(leftTable+"."+leftColumn),
//		"=",
//		NewColumn(rightTable+"."+rightColumn),
//	)
//}

// Helper to release node slices when no longer needed
func ReleaseNodeSlice(nodes []Node) {
	for _, node := range nodes {
		if releasable, ok := node.(interface{ Release() }); ok {
			releasable.Release()
		}
	}
	putNodeSlice(nodes)
}
