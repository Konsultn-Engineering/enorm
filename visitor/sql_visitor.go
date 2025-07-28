package visitor

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"strconv"
	"strings"
	"sync"
)

type SQLVisitor struct {
	sb      strings.Builder
	args    []any
	dialect dialect.Dialect
	qcache  cache.QueryCache
	mu      sync.Mutex
}

func NewSQLVisitor(d dialect.Dialect, q cache.QueryCache) *SQLVisitor {
	return &SQLVisitor{
		dialect: d,
		qcache:  q,
	}
}

func (v *SQLVisitor) Reset() {
	v.sb.Reset()
	v.args = v.args[:0]
}

func (v *SQLVisitor) Build(root ast.Node) (string, []any, error) {
	fp := root.Fingerprint()

	// 1. Fast path: retrieve from cache
	if cached, ok := v.qcache.GetSQL(fp); ok {
		return cached.SQL, v.args, nil // assume args set via Accept
	}

	// 2. Slow path: render and cache
	v.sb.Reset()
	v.args = v.args[:0]

	if err := root.Accept(v); err != nil {
		return "", nil, err
	}

	sql := v.sb.String()
	v.qcache.SetSQL(fp, &cache.CachedQuery{SQL: sql})
	return sql, v.args, nil
}

func (v *SQLVisitor) Arg(a any) {
	v.args = append(v.args, a)
}

func (v *SQLVisitor) VisitSelect(s *ast.SelectStmt) error {
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
