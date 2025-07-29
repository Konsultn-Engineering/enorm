package engine

import (
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/visitor"
)

// Test just the query building part without database
func BenchmarkQueryBuildingOnly(b *testing.B) {
	v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

		_, _, _ = v.Build(selectStmt)
		v.Reset()
	}
	b.ReportAllocs()
}

func BenchmarkQueryBuildingWithPooling(b *testing.B) {
	qcache := cache.NewQueryCache()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := visitor.NewSQLVisitor(dialect.Postgres{}, qcache)
		
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

		_, _, _ = v.Build(selectStmt)
		v.Release()
	}
	b.ReportAllocs()
}