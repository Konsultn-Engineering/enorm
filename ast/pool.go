package ast

import (
	"sync"
)

// Enhanced pools with node slice pooling
var (
	selectStmtPool = sync.Pool{
		New: func() any {
			return &SelectStmt{
				Columns: make([]Node, 0, 50),
				Joins:   make([]*JoinClause, 0, 10),
			}
		},
	}

	whereConditionPool = sync.Pool{
		New: func() interface{} {
			return &WhereCondition{}
		},
	}

	columnPool = sync.Pool{
		New: func() any { return &Column{} },
	}

	tablePool = sync.Pool{
		New: func() any { return &Table{} },
	}

	valuePool = sync.Pool{
		New: func() any { return &Value{} },
	}

	binaryExprPool = sync.Pool{
		New: func() any { return &BinaryExpr{} },
	}

	unaryExprPool = sync.Pool{
		New: func() any { return &UnaryExpr{} },
	}

	limitClausePool = sync.Pool{
		New: func() any { return &LimitClause{} },
	}

	orderByClausePool = sync.Pool{
		New: func() interface{} {
			return &OrderByClause{}
		},
	}

	arrayPool = sync.Pool{
		New: func() any {
			return &Array{Values: make([]Value, 0, 100)}
		},
	}

	groupByClausePool = sync.Pool{
		New: func() any {
			return &GroupByClause{Exprs: make([]Node, 0, 50)}
		},
	}

	joinConditionPool = sync.Pool{
		New: func() any {
			return &JoinCondition{}
		},
	}

	joinConditionNodePool = sync.Pool{
		New: func() any {
			return &JoinConditionNode{}
		},
	}

	joinClausePool = sync.Pool{
		New: func() any { return &JoinClause{} },
	}

	subqueryExprPool = sync.Pool{
		New: func() any { return &SubqueryExpr{} },
	}

	// NEW: Node slice pools for different sizes
	nodeSlicePool8 = sync.Pool{
		New: func() any {
			return make([]Node, 0, 8)
		},
	}

	nodeSlicePool16 = sync.Pool{
		New: func() any {
			return make([]Node, 0, 16)
		},
	}

	conditionSlicePool = sync.Pool{
		New: func() any {
			return make([]Node, 0, 8)
		},
	}

	operatorSlicePool = sync.Pool{
		New: func() any {
			return make([]string, 0, 8)
		},
	}

	functionPool = sync.Pool{
		New: func() any {
			return &Function{
				Name: "",
				Args: make([]Node, 0, 10), // Preallocate with a reasonable size
			}
		},
	}
)

// Helper to get appropriately sized node slice
func getNodeSlice(size int) []Node {
	if size <= 8 {
		slice := nodeSlicePool8.Get().([]Node)
		return slice[:0]
	}
	slice := nodeSlicePool16.Get().([]Node)
	return slice[:0]
}

func putNodeSlice(slice []Node) {
	if cap(slice) <= 8 {
		nodeSlicePool8.Put(slice)
	} else {
		nodeSlicePool16.Put(slice)
	}
}
