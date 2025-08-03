package ast

import (
	"sync"
	"testing"
)

// Benchmark AST node creation and release
func BenchmarkColumnPool(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			col := NewColumn("test_column")
			col.Release()
		}
	})
}

func BenchmarkColumnPoolReuse(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := NewColumn("test_column")
		col.Name = "dynamic_name"
		col.Table = "users"
		col.Alias = "u"
		col.Release()
	}
}

func BenchmarkColumnDirect(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col := &Column{
			Name:  "test_column",
			Table: "users",
			Alias: "u",
		}
		_ = col
	}
}

func BenchmarkSelectStmtPool(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stmt := NewSelectStmt()
			stmt.Columns = append(stmt.Columns, NewColumn("id"))
			stmt.Columns = append(stmt.Columns, NewColumn("name"))
			stmt.From = NewTable("users")
			stmt.Release()
		}
	})
}

func BenchmarkSelectStmtDirect(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt := &SelectStmt{
			Columns: []Node{
				&Column{Name: "id"},
				&Column{Name: "name"},
			},
			From: &Table{Name: "users"},
		}
		_ = stmt
	}
}

func BenchmarkComplexASTPooled(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt := NewSelectStmt()
		stmt.Columns = []Node{
			NewColumn("id"),
			NewColumn("name"),
			NewColumn("email"),
		}
		stmt.From = NewTable("users")
		stmt.Where = NewWhereClause(
			NewBinaryExpr(
				NewColumn("active"),
				"=",
				NewValue(true),
			),
		)
		stmt.OrderBy = []*OrderByClause{
			NewOrderByClause(NewColumn("created_at"), true),
		}
		stmt.Limit = NewLimitClause(&[]int{10}[0], nil)
		stmt.Release()
	}
}

func BenchmarkComplexASTDirect(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt := &SelectStmt{
			Columns: []Node{
				&Column{Name: "id"},
				&Column{Name: "name"},
				&Column{Name: "email"},
			},
			From: &Table{Name: "users"},
			Where: &WhereClause{
				Condition: &BinaryExpr{
					Left:     &Column{Name: "active"},
					Operator: "=",
					Right:    &Value{Val: true},
				},
			},
			OrderBy: []*OrderByClause{
				{Expr: &Column{Name: "created_at"}, Desc: true},
			},
			Limit: &LimitClause{Count: &[]int{10}[0]},
		}
		_ = stmt
	}
}

// Memory allocation benchmarks
func BenchmarkMemoryAllocation(b *testing.B) {
	b.Run("Pooled", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col := NewColumn("test")
			value := NewValue(123)
			expr := NewBinaryExpr(col, "=", value)
			where := NewWhereClause(expr)

			where.Release()
		}
	})

	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			col := &Column{Name: "test"}
			table := &Table{Name: "users"}
			value := &Value{Val: 123}
			expr := &BinaryExpr{Left: col, Operator: "=", Right: value}
			where := &WhereClause{Condition: expr}

			_ = table
			_ = where
		}
	})
}

// Concurrent access benchmarks
func BenchmarkConcurrentPoolAccess(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			col := NewColumn("test")
			table := NewTable("users")
			stmt := NewSelectStmt()
			stmt.Columns = []Node{col}
			stmt.From = table
			stmt.Release()
		}
	})
}

// Pool pressure test
func BenchmarkPoolPressure(b *testing.B) {
	const numGoroutines = 100
	const opsPerGoroutine = 1000

	b.ResetTimer()
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				stmt := NewSelectStmt()
				for k := 0; k < 5; k++ {
					stmt.Columns = append(stmt.Columns, NewColumn("col"))
				}
				stmt.From = NewTable("table")
				stmt.Where = NewWhereClause(
					NewBinaryExpr(
						NewColumn("id"),
						"=",
						NewValue(j),
					),
				)
				stmt.Release()
			}
		}()
	}

	wg.Wait()
}
