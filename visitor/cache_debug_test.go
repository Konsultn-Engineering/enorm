package visitor

import (
	"fmt"
	"testing"

	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
)

func TestCacheDebug(t *testing.T) {
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

	// First call should cache
	sql1, args1, err1 := visitor.Build(stmt)
	fmt.Printf("First call: SQL=%s, args=%v, err=%v\n", sql1, args1, err1)
	visitor.Reset()

	// Second call should use cache
	sql2, args2, err2 := visitor.Build(stmt)
	fmt.Printf("Second call: SQL=%s, args=%v, err=%v\n", sql2, args2, err2)
	visitor.Reset()

	if sql1 != sql2 {
		t.Errorf("SQL mismatch: %s != %s", sql1, sql2)
	}
}