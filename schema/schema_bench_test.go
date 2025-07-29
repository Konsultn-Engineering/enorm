package schema

import (
	"reflect"
	"testing"
	"time"
)

type TestUser struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type mockRowScanner struct {
	columns []string
	values  []any
	scanCalled bool
}

func (m *mockRowScanner) Scan(dest ...any) error {
	m.scanCalled = true
	for i, d := range dest {
		if i < len(m.values) {
			v := reflect.ValueOf(d).Elem()
			v.Set(reflect.ValueOf(m.values[i]))
		}
	}
	return nil
}

func (m *mockRowScanner) Columns() ([]string, error) {
	return m.columns, nil
}

func BenchmarkSchemaScanning(b *testing.B) {
	// Register scanner
	RegisterScanner(TestUser{}, func(a any, scanner FieldRegistry) error {
		u := a.(*TestUser)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	// Get scanner function
	t := reflect.TypeOf(TestUser{})
	scannerFn := getRegisteredScanner(t)

	row := &mockRowScanner{
		columns: []string{"id", "first_name", "email", "created_at", "updated_at"},
		values:  []any{uint64(1), "John", "john@example.com", time.Now(), time.Now()},
	}

	user := &TestUser{}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scannerFn(user, row)
	}
	b.ReportAllocs()
}