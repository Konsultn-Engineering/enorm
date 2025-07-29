package engine

import (
	"reflect"
	"testing"
	"time"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/Konsultn-Engineering/enorm/visitor"
)

type BenchUser struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func BenchmarkCompleteFindOneSimulation(b *testing.B) {
	// Create an optimized engine
	e := &Engine{
		qcache: cache.NewQueryCache(),
	}
	
	// Register schema once
	schema.RegisterScanner(BenchUser{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*BenchUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &BenchUser{}
		
		// Schema introspection (this will be cached internally by schema package)
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		
		// Create cache key for this query type
		cacheKey := "findone:" + meta.Plural
		
		// Check AST cache
		var selectStmt *ast.SelectStmt
		if cached, ok := e.astCache.Load(cacheKey); ok {
			selectStmt = cached.(*ast.SelectStmt)
		} else {
			// Build and cache AST
			selectStmt = &ast.SelectStmt{
				Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
				From:    &ast.Table{Name: meta.Plural},
				Limit:   &ast.LimitClause{Count: ptr(1)},
			}
			e.astCache.Store(cacheKey, selectStmt)
		}

		// Get visitor from pool
		v := visitor.NewSQLVisitor(dialect.Postgres{}, e.qcache)
		_, _, _ = v.Build(selectStmt)
		v.Release()
	}
	b.ReportAllocs()
}

func BenchmarkOriginalStyleFindOne(b *testing.B) {
	// Simulate original engine without caching
	v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	
	// Register schema once
	schema.RegisterScanner(BenchUser{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*BenchUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &BenchUser{}
		
		// Schema introspection
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		
		// Create AST every time (original style)
		cols := make([]ast.Node, 5)
		cols[0] = &ast.Column{Name: "id"}
		cols[1] = &ast.Column{Name: "first_name"}
		cols[2] = &ast.Column{Name: "email"}
		cols[3] = &ast.Column{Name: "created_at"}
		cols[4] = &ast.Column{Name: "updated_at"}
		
		selectStmt := &ast.SelectStmt{
			Columns: cols,
			From:    &ast.Table{Name: meta.Plural},
			Limit:   &ast.LimitClause{Count: ptr(1)},
		}

		// Build SQL
		_, _, _ = v.Build(selectStmt)
		v.Reset()
	}
	b.ReportAllocs()
}