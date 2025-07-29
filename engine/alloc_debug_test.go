package engine

import (
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/visitor"
)

func BenchmarkJustASTConstruction(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = &ast.SelectStmt{
			Columns: []ast.Node{
				&ast.Column{Name: "id"},
				&ast.Column{Name: "first_name"},
				&ast.Column{Name: "email"},
				&ast.Column{Name: "created_at"},
				&ast.Column{Name: "updated_at"},
			},
			From:  &ast.Table{Name: "users"},
			Limit: &ast.LimitClause{Count: ptr(1)},
		}
	}
	b.ReportAllocs()
}

func BenchmarkJustVisitorCall(b *testing.B) {
	v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	selectStmt := &ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "first_name"},
			&ast.Column{Name: "email"},
			&ast.Column{Name: "created_at"},
			&ast.Column{Name: "updated_at"},
		},
		From:  &ast.Table{Name: "users"},
		Limit: &ast.LimitClause{Count: ptr(1)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = v.Build(selectStmt)
		v.Reset()
	}
	b.ReportAllocs()
}