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

type TestUser struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func BenchmarkEngineQueryBuildingOnly(b *testing.B) {
	// Create an engine without database
	e := &Engine{
		visitor: visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache()),
	}
	
	// Register test schema
	schema.RegisterScanner(TestUser{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*TestUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	user := &TestUser{}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate the query building part of FindOne
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		
		// Create a cache key for this query type
		cacheKey := "findone:" + meta.Plural
		
		// Check if we have cached AST for this query
		var selectStmt *ast.SelectStmt
		if cached, ok := e.astCache.Load(cacheKey); ok {
			selectStmt = cached.(*ast.SelectStmt)
		} else {
			// Build new AST and cache it
			selectStmt = &ast.SelectStmt{
				Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
				From:    &ast.Table{Name: meta.Plural},
				Limit:   &ast.LimitClause{Count: ptr(1)},
			}
			e.astCache.Store(cacheKey, selectStmt)
		}

		_, _, _ = e.visitor.Build(selectStmt)
		e.visitor.Reset()
	}
	b.ReportAllocs()
}

func BenchmarkEngineQueryBuildingCached(b *testing.B) {
	// Create an engine without database
	e := &Engine{
		visitor: visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache()),
	}
	
	// Register test schema
	schema.RegisterScanner(TestUser{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*TestUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	user := &TestUser{}
	
	// Prime the cache
	meta, _ := schema.Introspect(reflect.TypeOf(user))
	cacheKey := "findone:" + meta.Plural
	selectStmt := &ast.SelectStmt{
		Columns: e.astFromCols([]string{"id", "first_name", "email", "created_at", "updated_at"}),
		From:    &ast.Table{Name: meta.Plural},
		Limit:   &ast.LimitClause{Count: ptr(1)},
	}
	e.astCache.Store(cacheKey, selectStmt)
	// Also prime the SQL cache
	e.visitor.Build(selectStmt)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate the query building part of FindOne
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		
		// Create a cache key for this query type
		cacheKey := "findone:" + meta.Plural
		
		// Get cached AST
		cached, _ := e.astCache.Load(cacheKey)
		selectStmt := cached.(*ast.SelectStmt)

		_, _, _ = e.visitor.Build(selectStmt)
		e.visitor.Reset()
	}
	b.ReportAllocs()
}