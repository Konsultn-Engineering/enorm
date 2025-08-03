package query

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"testing"

	"github.com/Konsultn-Engineering/enorm/schema"
)

type BenchUser struct {
	ID        int64  `db:"id"`
	Name      string `db:"name"`
	Email     string `db:"email"`
	Active    bool   `db:"active"`
	CreatedAt string `db:"created_at"`
}

func setupBenchContext() *schema.Context {
	return schema.New()
}

// Simple query benchmarks
func BenchmarkSimpleSelect(b *testing.B) {
	ctx := setupBenchContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		builder := Select[BenchUser](ctx)
		sql, args, err := builder.ToSQL()
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
		_ = args
		builder.Release()
	}
}

func BenchmarkSimpleSelectWithWhere(b *testing.B) {
	ctx := setupBenchContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		builder := Select[BenchUser](ctx)
		builder.Where("id", int64(123))
		sql, args, err := builder.ToSQL()
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
		_ = args
		builder.Release()
	}
}

func BenchmarkComplexSelect(b *testing.B) {
	ctx := setupBenchContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		builder := Select[BenchUser](ctx)
		builder.
			Columns("id", "name", "email").
			Where("active", true).
			Where("name", "John").
			WhereIn("status", []any{"active", "pending"}).
			OrderByDesc("created_at").
			OrderByAsc("name").
			Limit(10)

		sql, args, err := builder.ToSQL()
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
		_ = args
		builder.Release()
	}
}

func BenchmarkSelectWithJoins(b *testing.B) {
	ctx := setupBenchContext()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		builder := Select[BenchUser](ctx)
		builder.
			Columns("u.id", "u.name", "p.title").
			InnerJoin("posts", "id", "user_id").
			LeftJoin("comments", "id", "user_id").
			Where("u.active", true).
			OrderByDesc("u.created_at").
			Limit(20)

		sql, args, err := builder.ToSQL()
		if err != nil {
			b.Fatal(err)
		}
		_ = sql
		_ = args
		builder.Release()
	}
}

// Parameter handling benchmarks
func BenchmarkParameterHandling(b *testing.B) {
	ctx := setupBenchContext()

	b.Run("SmallParams", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := Select[BenchUser](ctx)
			builder.Where("id", 1).Where("active", true)
			sql, args, err := builder.ToSQL()
			if err != nil {
				b.Fatal(err)
			}
			_ = sql
			_ = args
			builder.Release()
		}
	})

	b.Run("ManyParams", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := Select[BenchUser](ctx)
			// Add 12 parameters (more than stack allocation limit of 8)
			for j := 0; j < 12; j++ {
				builder.Where("col"+string(rune('0'+j)), j)
			}
			sql, args, err := builder.ToSQL()
			if err != nil {
				b.Fatal(err)
			}
			_ = sql
			_ = args
			builder.Release()
		}
	})
}

// Builder pool benchmarks
func BenchmarkBuilderPooling(b *testing.B) {
	ctx := setupBenchContext()

	b.Run("WithPooling", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				builder := Select[BenchUser](ctx)
				builder.Where("id", 123)
				sql, args, err := builder.ToSQL()
				if err != nil {
					b.Fatal(err)
				}
				_ = sql
				_ = args
				builder.Release()
			}
		})
	})

	b.Run("WithoutPooling", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Direct allocation without pooling
				builder := &SelectBuilder[BenchUser]{
					stmt: &ast.SelectStmt{
						Columns: []ast.Node{&ast.Column{Name: "*"}},
						From:    &ast.Table{Name: "bench_users"},
						Where: &ast.WhereClause{
							Condition: &ast.BinaryExpr{
								Left:     &ast.Column{Name: "id"},
								Operator: "=",
								Right:    &ast.Value{Val: 123},
							},
						},
					},
					ctx: ctx,
				}

				sql, args, err := builder.ToSQL()
				if err != nil {
					b.Fatal(err)
				}
				_ = sql
				_ = args
			}
		})
	})
}

// Method chaining benchmarks
func BenchmarkMethodChaining(b *testing.B) {
	ctx := setupBenchContext()

	b.Run("ShortChain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := Select[BenchUser](ctx).
				Where("active", true).
				Limit(10)

			sql, args, err := builder.ToSQL()
			if err != nil {
				b.Fatal(err)
			}
			_ = sql
			_ = args
			builder.Release()
		}
	})

	b.Run("LongChain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := Select[BenchUser](ctx).
				Columns("id", "name", "email", "active", "created_at").
				Where("active", true).
				Where("deleted_at", nil).
				WhereIn("role", []any{"admin", "user", "moderator"}).
				WhereLike("name", "%john%").
				OrderByDesc("created_at").
				OrderByAsc("name").
				Limit(25).
				LimitOffset(25, 50)

			sql, args, err := builder.ToSQL()
			if err != nil {
				b.Fatal(err)
			}
			_ = sql
			_ = args
			builder.Release()
		}
	})
}

// Concurrent usage benchmarks
func BenchmarkConcurrentUsage(b *testing.B) {
	ctx := setupBenchContext()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			builder := Select[BenchUser](ctx)
			builder.
				Where("active", true).
				Where("id", 123).
				OrderByDesc("created_at").
				Limit(10)

			sql, args, err := builder.ToSQL()
			if err != nil {
				b.Fatal(err)
			}
			_ = sql
			_ = args
			builder.Release()
		}
	})
}
