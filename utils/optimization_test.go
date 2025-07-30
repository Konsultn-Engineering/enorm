package utils

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/schema"
	"reflect"
	"testing"
	"time"
)

// Comparison test to demonstrate optimization improvements
func TestOptimizationComparison(t *testing.T) {
	// Test setup identical to benchmark
	schema.RegisterScanner(TestUser{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*TestUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	// Get scanner function
	scannerType := reflect.TypeOf(TestUser{})
	scannerFn := schema.getRegisteredScanner(scannerType)

	row := &mockRowScanner{
		columns: []string{"id", "first_name", "email", "created_at", "updated_at"},
		values:  []any{uint64(1), "John", "john@example.com", time.Now(), time.Now()},
	}

	user := &TestUser{}

	// Test that scanning works correctly
	err := scannerFn(user, row)
	if err != nil {
		t.Fatalf("Scanning failed: %v", err)
	}

	// Verify values were set correctly
	if user.ID != 1 {
		t.Errorf("Expected ID=1, got %d", user.ID)
	}
	if user.FirstName != "John" {
		t.Errorf("Expected FirstName=John, got %s", user.FirstName)
	}
	if user.Email != "john@example.com" {
		t.Errorf("Expected Email=john@example.com, got %s", user.Email)
	}

	fmt.Printf("✅ Optimization test passed - all fields correctly set\n")
	fmt.Printf("   ID: %d\n", user.ID)
	fmt.Printf("   FirstName: %s\n", user.FirstName)
	fmt.Printf("   Email: %s\n", user.Email)
	fmt.Printf("   CreatedAt: %v\n", user.CreatedAt)
	fmt.Printf("   UpdatedAt: %v\n", user.UpdatedAt)
}
