package engine

import (
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/visitor"
)

// BenchmarkOriginalQueryBuildingApproach simulates the original approach
func BenchmarkOriginalQueryBuildingApproach(b *testing.B) {
	// Every operation creates new visitor and AST
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create new visitor each time (original approach)
		v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
		
		// Create new AST each time
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
		// No pooling - just discard
	}
	b.ReportAllocs()
}

// BenchmarkOptimizedQueryBuildingApproach simulates our optimized approach
func BenchmarkOptimizedQueryBuildingApproach(b *testing.B) {
	// Simulate the optimized engine with all caching
	e := &Engine{
		qcache: cache.NewQueryCache(),
	}
	
	// Pre-cache the AST and SQL
	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
		From:    &ast.Table{Name: "users"},
		Limit:   &ast.LimitClause{Count: ptr(1)},
	}
	e.astCache.Store("findone:users", selectStmt)
	
	// Prime the SQL cache too
	v := visitor.NewSQLVisitor(dialect.Postgres{}, e.qcache)
	v.Build(selectStmt)
	v.Release()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Get cached AST
		cached, _ := e.astCache.Load("findone:users")
		selectStmt := cached.(*ast.SelectStmt)
		
		// Use visitor pooling
		v := visitor.NewSQLVisitor(dialect.Postgres{}, e.qcache)
		_, _, _ = v.Build(selectStmt) // This should use cached SQL
		v.Release()
	}
	b.ReportAllocs()
}