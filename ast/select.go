package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type SelectStmt struct {
	Distinct    bool
	Columns     []Node
	From        *Table
	Joins       []*JoinClause
	Where       *WhereClause
	GroupBy     *GroupByClause
	Having      *WhereClause
	OrderBy     *OrderByClause
	OrderByTail *OrderByClause
	Limit       *LimitClause
	ForUpdate   bool

	// Internal condition management for optimization
	whereConditions []Node
	whereOperators  []string
	whereBuilt      bool
}

func NewSelectStmt() *SelectStmt {
	s := selectStmtPool.Get().(*SelectStmt)
	s.Columns = s.Columns[:0]
	s.From = nil
	s.Joins = s.Joins[:0]
	s.Where = nil
	s.GroupBy = nil
	s.Having = nil
	s.OrderBy = nil
	s.Limit = nil
	s.ForUpdate = false

	// Reset condition tracking
	s.whereBuilt = false

	// Always release and clear conditions
	if s.whereConditions != nil {
		s.releaseConditionSlices()
	}
	s.whereConditions = s.whereConditions[:0]

	return s
}

func (s *SelectStmt) Type() NodeType         { return NodeSelect }
func (s *SelectStmt) Accept(v Visitor) error { return v.VisitSelect(s) }
func (s *SelectStmt) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("select:"))
	if s.From != nil {
		h.Write(utils.U64ToBytes(s.From.Fingerprint()))
	}
	for _, col := range s.Columns {
		h.Write(utils.U64ToBytes(col.Fingerprint()))
	}
	//for _, j := range s.Joins {
	//	h.Write(utils.U64ToBytes(j.Fingerprint()))
	//}

	if s.Where != nil {
		h.Write(utils.U64ToBytes(s.Where.Fingerprint()))
	}
	if s.GroupBy != nil {
		h.Write(utils.U64ToBytes(s.GroupBy.Fingerprint()))
	}
	if s.Having != nil {
		h.Write(utils.U64ToBytes(s.Having.Fingerprint()))
	}
	//for _, o := range s.OrderBy {
	//	h.Write(utils.U64ToBytes(o.Fingerprint()))
	//}
	if s.Limit != nil {
		h.Write(utils.U64ToBytes(s.Limit.Fingerprint()))
	}
	if s.ForUpdate {
		h.Write([]byte("for_update"))
	}
	return h.Sum64()
}
func (s *SelectStmt) AddWhereCondition(condition Node, operator string) {
	newCondition := NewWhereClause(condition, operator)

	if s.Where == nil {
		s.Where = &WhereClause{
			First: newCondition,
			Tail:  newCondition,
		}
	} else {
		s.Where.Tail.Next = newCondition
		s.Where.Tail = newCondition
	}
}

func (s *SelectStmt) AddOrderByClause(table string, desc bool, columns ...string) {
	if len(columns) == 0 {
		return
	}

	for i, col := range columns {
		newClause := NewOrderByClause(NewColumn(table, col, ""), desc)

		// Mark last column in group
		if i == len(columns)-1 {
			newClause.IsGroupEnd = true
		}

		if s.OrderBy == nil {
			s.OrderBy = newClause
			s.OrderByTail = newClause
		} else {
			s.OrderByTail.Next = newClause
			s.OrderByTail = newClause
		}
	}
}

func (s *SelectStmt) AddJoinClause(joinType JoinType, schema, name, alias string) {
	join := NewJoinClause(joinType, schema, name, alias)
	n := len(s.Joins)
	if n < cap(s.Joins) {
		s.Joins = s.Joins[:n+1]
		s.Joins[n] = join
		return
	}
	s.Joins = append(s.Joins, join)
}

func (s *SelectStmt) AddJoinCondition(operator string, condition Node) {
	if len(s.Joins) == 0 {
		return // No joins to add conditions to
	}

	// Get the last join clause
	lastJoin := s.Joins[len(s.Joins)-1]

	lastJoin.Conditions.Append(operator, condition)
}

//	func (s *SelectStmt) BuildWhere() {
//		if s.whereBuilt || len(s.whereConditions) == 0 {
//			return
//		}
//
//		if len(s.whereConditions) == 1 {
//			s.Where = NewWhereClause(s.whereConditions[0])
//		} else {
//			// Build binary tree of conditions
//			current := s.whereConditions[0]
//
//			for i := 1; i < len(s.whereConditions); i++ {
//				operator := s.whereOperators[i-1]
//				current = NewBinaryExpr(current, operator, s.whereConditions[i])
//			}
//
//			s.Where = NewWhereClause(current)
//		}
//
//		s.whereBuilt = true
//	}
//
//	func (s *SelectStmt) RebuildWhere() {
//		if s.Where != nil {
//			s.Where.Release()
//			s.Where = nil
//		}
//		s.whereBuilt = false
//		s.BuildWhere()
//	}
func (s *SelectStmt) releaseConditionSlices() {
	if s.whereConditions != nil {
		for _, cond := range s.whereConditions {
			if releasable, ok := cond.(interface{ Release() }); ok {
				releasable.Release()
			}
		}
		conditionSlicePool.Put(s.whereConditions[:0])
		s.whereConditions = nil
	}

	if s.whereOperators != nil {
		operatorSlicePool.Put(s.whereOperators[:0])
		s.whereOperators = nil
	}
}
func (s *SelectStmt) Release() {
	// Release child nodes
	for _, col := range s.Columns {
		if releasable, ok := col.(interface{ Release() }); ok {
			releasable.Release()
		}
	}
	if s.From != nil {
		s.From.Release()
	}

	for _, join := range s.Joins {
		join.Release()
	}

	if s.Where != nil {
		s.Where.Release()
	}
	if s.GroupBy != nil {
		s.GroupBy.Release()
	}

	if s.OrderBy != nil {
		s.OrderBy.Release()
	}

	if s.Limit != nil {
		s.Limit.Release()
	}

	s.releaseConditionSlices()

	s.Columns = s.Columns[:0] // Clear but keep capacity
	s.Joins = s.Joins[:0]     // Clear but keep capacity
	s.OrderBy = nil
	s.Where = nil // ← This is key!

	// Clear single references
	s.From = nil
	s.Where = nil
	s.GroupBy = nil
	s.Limit = nil

	selectStmtPool.Put(s)
}
