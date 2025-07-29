package visitor

import (
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

func BenchmarkVisitorBuild(b *testing.B) {
	visitor := NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	stmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
			&ast.Column{Name: "email"},
			&ast.Column{Name: "created_at"},
			&ast.Column{Name: "updated_at"},
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

func BenchmarkVisitorBuildCached(b *testing.B) {
	visitor := NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	stmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
			&ast.Column{Name: "email"},
			&ast.Column{Name: "created_at"},
			&ast.Column{Name: "updated_at"},
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

	// Prime the cache
	visitor.Build(stmt)
	visitor.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = visitor.Build(stmt)
		visitor.Reset()
	}
	b.ReportAllocs()
}

func ptr[T any](v T) *T { return &v }