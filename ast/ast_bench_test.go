package ast

import (
	"testing"
)

func BenchmarkSelectStmtFingerprint(b *testing.B) {
	stmt := &SelectStmt{
		Columns: []Node{
			&Column{Name: "id"},
			&Column{Name: "first_name"},
			&Column{Name: "email"},
			&Column{Name: "created_at"},
			&Column{Name: "updated_at"},
		},
		From: &Table{Name: "users"},
		Limit: &LimitClause{Count: ptr(1)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stmt.Fingerprint()
	}
	b.ReportAllocs()
}

func BenchmarkSelectStmtConstruction(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = &SelectStmt{
			Columns: []Node{
				&Column{Name: "id"},
				&Column{Name: "first_name"},
				&Column{Name: "email"},
				&Column{Name: "created_at"},
				&Column{Name: "updated_at"},
			},
			From:  &Table{Name: "users"},
			Limit: &LimitClause{Count: ptr(1)},
		}
	}
	b.ReportAllocs()
}

func BenchmarkSelectStmtConstructionPooled(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt := NewSelectStmt()
		
		// Pre-size the columns slice to avoid reallocation
		if cap(stmt.Columns) < 5 {
			stmt.Columns = make([]Node, 0, 5)
		}
		
		stmt.Columns = append(stmt.Columns,
			NewColumn("id"),
			NewColumn("first_name"),
			NewColumn("email"),
			NewColumn("created_at"),
			NewColumn("updated_at"),
		)
		stmt.From = NewTable("users")
		stmt.Limit = NewLimitClause(ptr(1))
		stmt.Release()
	}
	b.ReportAllocs()
}

func ptr[T any](v T) *T { return &v }