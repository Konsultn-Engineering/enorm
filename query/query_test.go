package query

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"testing"
)

func TestWhereClauseLinkedList(t *testing.T) {
	// Create a SELECT statement
	stmt := &ast.SelectStmt{}

	// Add multiple WHERE conditions
	stmt.AddWhereCondition(&ast.Column{Name: "age"}, "AND")
	stmt.AddWhereCondition(&ast.Column{Name: "status"}, "OR")
	stmt.AddWhereCondition(&ast.Column{Name: "active"}, "AND")

	// Create visitor and generate SQL
	v := visitor.NewSQLVisitor(dialect.NewPostgresDialect(), cache.NewQueryCache())
	err := v.VisitWhereClause(stmt.Where)

	if err != nil {
		t.Fatalf("Error visiting WHERE clause: %v", err)
	}

	expected := " WHERE \"age\" OR \"status\" AND \"active\""
	actual := v.GetSB().String()

	if actual != expected {
		t.Errorf("Expected: %s, Got: %s", expected, actual)
	}

	// Test cleanup
	stmt.Where.Release()

	fmt.Printf("✓ WHERE clause generated: %s\n", actual)
}
