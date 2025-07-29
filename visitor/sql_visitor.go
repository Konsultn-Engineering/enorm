package visitor

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"strconv"
	"strings"
	"sync"
)

var visitorPool = sync.Pool{
	New: func() any {
		return &SQLVisitor{
			args: make([]any, 0, 8),
		}
	},
}

var argsPool = sync.Pool{
	New: func() any {
		return make([]any, 0, 8)
	},
}

type SQLVisitor struct {
	sb      strings.Builder
	args    []any
	dialect dialect.Dialect
	qcache  cache.QueryCache
	mu      sync.Mutex
}

func NewSQLVisitor(d dialect.Dialect, q cache.QueryCache) *SQLVisitor {
	v := visitorPool.Get().(*SQLVisitor)
	v.dialect = d
	v.qcache = q
	v.sb.Reset()
	v.args = v.args[:0]
	return v
}

func (v *SQLVisitor) Release() {
	v.dialect = nil
	v.qcache = nil
	v.sb.Reset()
	v.args = v.args[:0]
	visitorPool.Put(v)
}

func (v *SQLVisitor) Reset() {
	v.sb.Reset()
	v.args = v.args[:0]
}

func (v *SQLVisitor) Build(root ast.Node) (string, []any, error) {
	fp := root.Fingerprint()

	// 1. Fast path: retrieve from cache
	if cached, ok := v.qcache.GetSQL(fp); ok {
		// Return cached SQL and args, ensure args slice is properly set
		if cached.Args != nil {
			return cached.SQL, cached.Args, nil
		}
		return cached.SQL, nil, nil
	}

	// 2. Slow path: render and cache
	v.sb.Reset()
	v.args = v.args[:0]

	if err := root.Accept(v); err != nil {
		return "", nil, err
	}

	sql := v.sb.String()
	// Cache both SQL and args - use pooled slice for args copy
	var argsCopy []any
	if len(v.args) > 0 {
		argsCopy = argsPool.Get().([]any)
		if cap(argsCopy) < len(v.args) {
			argsCopy = make([]any, len(v.args))
		} else {
			argsCopy = argsCopy[:len(v.args)]
		}
		copy(argsCopy, v.args)
	}
	
	v.qcache.SetSQL(fp, &cache.CachedQuery{
		SQL:  sql,
		Args: argsCopy,
	})
	return sql, v.args, nil
}

func (v *SQLVisitor) Arg(a any) {
	v.args = append(v.args, a)
}

func (v *SQLVisitor) VisitSelect(s *ast.SelectStmt) error {
	//	SELECT
	//	[DISTINCT] column_list
	//	FROM
	//	table_name
	//[JOIN ...]
	//	[WHERE condition]
	//	[GROUP BY column_list]
	//	[HAVING condition]
	//	[WINDOW ...]
	//	[ORDER BY column_list]
	//	[LIMIT count]
	//	[OFFSET count]
	//	[FETCH FIRST ... ROWS ONLY]
	//	[FOR UPDATE | FOR SHARE]

	v.sb.WriteString("SELECT ")

	for i, col := range s.Columns {
		if i > 0 {
			v.sb.WriteString(", ")
		}
		if err := col.Accept(v); err != nil {
			return err
		}
	}

	if s.From != nil {
		if err := s.From.Accept(v); err != nil {
			return err
		}
	}

	if s.Where != nil {
		if err := s.Where.Accept(v); err != nil {
			return err
		}
	}

	if s.GroupBy != nil {
		if err := s.GroupBy.Accept(v); err != nil {
			return err
		}
	}

	for i, ord := range s.OrderBy {
		if i > 0 {
			v.sb.WriteString(", ")
		}
		if err := ord.Accept(v); err != nil {
			return err
		}
	}

	if s.Limit != nil {
		if err := s.Limit.Accept(v); err != nil {
			return err
		}
	}

	return nil
}

func (v *SQLVisitor) VisitInsert(stmt *ast.InsertStmt) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitUpdate(stmt *ast.UpdateStmt) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitDelete(stmt *ast.DeleteStmt) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitCreateTable(stmt *ast.CreateTableStmt) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitColumn(c *ast.Column) error {
	if c.Table != "" {
		v.sb.WriteString(v.dialect.QuoteIdentifier(c.Table))
		v.sb.WriteByte('.')
	}
	v.sb.WriteString(v.dialect.QuoteIdentifier(c.Name))

	if c.Alias != "" && c.Alias != c.Name {
		v.sb.WriteString(" AS ")
		v.sb.WriteString(v.dialect.QuoteIdentifier(c.Alias))
	}

	return nil
}

func (v *SQLVisitor) VisitTable(t *ast.Table) error {
	v.sb.WriteString(" FROM ")

	if t.Schema != "" {
		v.sb.WriteString(v.dialect.QuoteIdentifier(t.Schema))
		v.sb.WriteByte('.')
	}
	v.sb.WriteString(v.dialect.QuoteIdentifier(t.Name))

	if t.Alias != "" && t.Alias != t.Name {
		v.sb.WriteString(" AS ")
		v.sb.WriteString(v.dialect.QuoteIdentifier(t.Alias))
	}

	return nil
}

func (v *SQLVisitor) VisitValue(val *ast.Value) error {
	// Either inline literal (e.g., true) or bind param ("?")
	placeholder := v.dialect.Placeholder(len(v.args) + 1)
	v.sb.WriteString(placeholder)
	v.Arg(val.Val)
	return nil
}

func (v *SQLVisitor) VisitArray(a *ast.Array) error {
	v.sb.WriteByte('(')
	for i, val := range a.Values {
		if i > 0 {
			v.sb.WriteString(", ")
		}
		v.sb.WriteString(v.dialect.Placeholder(len(v.args) + 1))
		v.Arg(val.Val)
	}
	v.sb.WriteByte(')')
	return nil
}

func (v *SQLVisitor) VisitFunction(function *ast.Function) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitGroupedExpr(g *ast.GroupedExpr) error {
	v.sb.WriteByte('(')
	err := g.Expr.Accept(v)
	v.sb.WriteByte(')')
	return err
}

func (v *SQLVisitor) VisitBinaryExpr(expr *ast.BinaryExpr) error {
	if err := expr.Left.Accept(v); err != nil {
		return err
	}

	v.sb.WriteByte(' ')
	v.sb.WriteString(expr.Operator)
	v.sb.WriteByte(' ')

	if err := expr.Right.Accept(v); err != nil {
		return err
	}

	return nil
}

func (v *SQLVisitor) VisitUnaryExpr(expr *ast.UnaryExpr) error {
	v.sb.WriteString(expr.Operator)
	v.sb.WriteByte(' ')
	return expr.Operand.Accept(v)
}

func (v *SQLVisitor) VisitSubqueryExpr(s *ast.SubqueryExpr) error {
	v.sb.WriteByte('(')
	err := s.Stmt.Accept(v)
	v.sb.WriteByte(')')
	return err
}

func (v *SQLVisitor) VisitWhereClause(w *ast.WhereClause) error {
	v.sb.WriteString(" WHERE ")
	return w.Condition.Accept(v)
}

func (v *SQLVisitor) VisitJoinClause(clause *ast.JoinClause) error {
	//TODO implement me
	return nil
}

func (v *SQLVisitor) VisitGroupBy(g *ast.GroupByClause) error {
	if len(g.Exprs) == 0 {
		return nil
	}
	v.sb.WriteString(" GROUP BY ")
	for i, expr := range g.Exprs {
		if i > 0 {
			v.sb.WriteString(", ")
		}
		if err := expr.Accept(v); err != nil {
			return err
		}
	}
	return nil
}

func (v *SQLVisitor) VisitOrderByClause(clause *ast.OrderByClause) error {
	err := clause.Expr.Accept(v)

	if clause.Desc {
		v.sb.WriteString(" DESC")
	}
	return err
}

func (v *SQLVisitor) VisitLimitClause(clause *ast.LimitClause) error {
	v.sb.WriteString(" LIMIT ")
	if clause.Count != nil {
		v.sb.WriteString(strconv.Itoa(*clause.Count))
	}

	if clause.Offset != nil {
		v.sb.WriteString(" OFFSET ")
		v.sb.WriteString(strconv.Itoa(*clause.Offset))
	}

	return nil
}

// stub for interface satisfaction
//type _ ast.Visitor = (*SQLVisitor)(nil)
