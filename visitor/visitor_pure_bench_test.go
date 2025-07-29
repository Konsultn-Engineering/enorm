package visitor

import (
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

func BenchmarkVisitorBuildNoCachePureFirst(b *testing.B) {
	visitor := NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	stmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
		},
		From: &ast.Table{Name: "users"},
		Where: &ast.WhereClause{
			Condition: &ast.BinaryExpr{
				Left:     &ast.Column{Name: "id"},
				Operator: "=",
				Right:    &ast.Value{Val: 123},
			},
		},
		Limit: &ast.LimitClause{Count: ptr(1)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = visitor.Build(stmt)
		visitor.Reset()
	}
	b.ReportAllocs()
}

func BenchmarkVisitorBuildPureCached(b *testing.B) {
	visitor := NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	stmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
		},
		From: &ast.Table{Name: "users"},
		Where: &ast.WhereClause{
			Condition: &ast.BinaryExpr{
				Left:     &ast.Column{Name: "id"},
				Operator: "=",
				Right:    &ast.Value{Val: 123},
			},
		},
		Limit: &ast.LimitClause{Count: ptr(1)},
	}

	// Prime the cache first
	_, _, _ = visitor.Build(stmt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = visitor.Build(stmt)
	}
	b.ReportAllocs()
}

func BenchmarkVisitorWithPooling(b *testing.B) {
	cache := cache.NewQueryCache()
	
	stmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
		},
		From: &ast.Table{Name: "users"},
		Where: &ast.WhereClause{
			Condition: &ast.BinaryExpr{
				Left:     &ast.Column{Name: "id"},
				Operator: "=",
				Right:    &ast.Value{Val: 123},
			},
		},
		Limit: &ast.LimitClause{Count: ptr(1)},
	}

	// Prime the cache first
	visitor := NewSQLVisitor(dialect.Postgres{}, cache)
	_, _, _ = visitor.Build(stmt)
	visitor.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := NewSQLVisitor(dialect.Postgres{}, cache)
		_, _, _ = v.Build(stmt)
		v.Release()
	}
	b.ReportAllocs()
}