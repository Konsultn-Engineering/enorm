package engine

import (
	"context"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/schema"
	"os"
	"reflect"
	"runtime/pprof"
	"testing"
	"time"
	"unsafe"

	"github.com/Konsultn-Engineering/enorm/connector"
	_ "github.com/Konsultn-Engineering/enorm/providers/postgres"
)

type User struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

var e *Engine

func init() {
	conn, err := connector.New("postgres", connector.Config{
		Host:     "localhost",
		Port:     5432,
		Database: "enorm_test",
		Username: "postgres",
		Password: "admin",
		SSLMode:  "disable",
		Pool: connector.PoolConfig{
			MaxOpen:     10,
			MaxIdle:     5,
			MaxLifetime: time.Hour,
		},
	})

	if err != nil {
		panic("Failed to init connector: " + err.Error())
	}
	db, err := conn.Connect(context.Background())
	db.DB().Exec("CREATE TABLE IF NOT EXISTS users (\n  id BIGSERIAL PRIMARY KEY,\n  first_name TEXT NOT NULL,\n  email TEXT NOT NULL,\n  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n);\n")
	db.DB().Exec("INSERT INTO users (first_name, email) VALUES ('sol', 'sol@sol.com')")
	if err != nil {
		panic("Failed to connect: " + err.Error())
	}
	e = New(db.DB())

}

//func BenchmarkFindOne(b *testing.B) {
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
//	})
//
//	u := User{}
//
//	// Warm-up or validate connection
//	_, _ = e.FindOne(&u)
//	runtime.GC()
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _ = e.FindOne(&u)
//	}
//
//	b.ReportAllocs()
//}

func BenchmarkFindOneWithProfile(b *testing.B) {
	// Create CPU profile
	f, err := os.Create("cpu.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	// Your existing benchmark code
	user := &User{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.FindOne(user)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchemaIntrospect(b *testing.B) {
	userType := reflect.TypeOf(User{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := schema.Introspect(userType)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetConverter(b *testing.B) {
	var zero string
	sourceType := reflect.TypeOf(int64(0))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := schema.GetConverter(zero, sourceType)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectSet(b *testing.B) {
	user := &User{}
	meta, _ := schema.Introspect(reflect.TypeOf(user))
	fieldMeta := meta.ColumnMap["first_name"]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fieldMeta.DirectSet(unsafe.Pointer(user), "John")
	}
}

func BenchmarkDatabaseQuery(b *testing.B) {
	db := e.db // Your DB setup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkRowScan(b *testing.B) {
	db := e.db
	rows, _ := db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
	defer rows.Close()

	scanVals := make([]any, 5)
	scanPtrs := make([]any, 5)
	for i := range scanVals {
		scanPtrs[i] = &scanVals[i]
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows.Next()
		err := rows.Scan(scanPtrs...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark your current FindOne step by step
func BenchmarkFindOneStepByStep(b *testing.B) {
	user := &User{}

	b.Run("Introspect", func(b *testing.B) {
		userType := reflect.TypeOf(user)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			schema.Introspect(userType)
		}
	})

	b.Run("QueryBuild", func(b *testing.B) {
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		cols := []string{"id", "first_name", "email", "created_at", "updated_at"}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			selectStmt := &ast.SelectStmt{
				Columns: e.astFromCols(cols),
				From:    &ast.Table{Name: meta.TableName},
				Limit:   &ast.LimitClause{Count: ptr(1)},
			}
			e.visitor.Build(selectStmt)
		}
	})

	b.Run("DatabaseExecution", func(b *testing.B) {
		query := "SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1"

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := e.db.Query(query)
			if err != nil {
				b.Fatal(err)
			}
			rows.Close()
		}
	})

	b.Run("ScanAndSet", func(b *testing.B) {
		meta, _ := schema.Introspect(reflect.TypeOf(user))
		cols := []string{"id", "first_name", "email", "created_at", "updated_at"}
		scanVals := []any{int64(1), "John", "john@example.com", time.Now(), time.Now()}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			freshUser := &User{}
			meta.ScanAndSet(freshUser, cols, scanVals)
		}
	})
}

func BenchmarkFindOneWithTiming(b *testing.B) {
	user := &User{}

	var (
		introspectTime time.Duration
		queryBuildTime time.Duration
		dbExecTime     time.Duration
		scanSetTime    time.Duration
		totalTime      time.Duration
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()

		// 1. Introspection
		introspectStart := time.Now()
		meta, err := schema.Introspect(reflect.TypeOf(user))
		if err != nil {
			b.Fatal(err)
		}
		introspectTime += time.Since(introspectStart)

		// 2. Query Building
		queryBuildStart := time.Now()
		cols := []string{"id", "first_name", "email", "created_at", "updated_at"}
		selectStmt := &ast.SelectStmt{
			Columns: e.astFromCols(cols),
			From:    &ast.Table{Name: meta.TableName},
			Limit:   &ast.LimitClause{Count: ptr(1)},
		}
		query, _, err := e.visitor.Build(selectStmt)
		if err != nil {
			b.Fatal(err)
		}
		queryBuildTime += time.Since(queryBuildStart)

		// 3. Database Execution
		dbExecStart := time.Now()
		rows, err := e.db.Query(query)
		if err != nil {
			b.Fatal(err)
		}

		if !rows.Next() {
			rows.Close()
			continue
		}

		scanVals := make([]any, len(cols))
		scanPtrs := make([]any, len(cols))
		for j := range scanVals {
			scanPtrs[j] = &scanVals[j]
		}

		err = rows.Scan(scanPtrs...)
		rows.Close()
		if err != nil {
			b.Fatal(err)
		}
		dbExecTime += time.Since(dbExecStart)

		// 4. Scan and Set
		scanSetStart := time.Now()
		err = meta.ScanAndSet(user, cols, scanVals)
		if err != nil {
			b.Fatal(err)
		}
		scanSetTime += time.Since(scanSetStart)

		totalTime += time.Since(start)
	}

	avgIntrospect := introspectTime / time.Duration(b.N)
	avgQueryBuild := queryBuildTime / time.Duration(b.N)
	avgDbExec := dbExecTime / time.Duration(b.N)
	avgScanSet := scanSetTime / time.Duration(b.N)
	avgTotal := totalTime / time.Duration(b.N)

	fmt.Printf("\nDetailed Timing Breakdown (avg per operation):\n")
	fmt.Printf("1. Introspect:   %v (%0.1f%%)\n", avgIntrospect, float64(introspectTime)/float64(totalTime)*100)
	fmt.Printf("2. Query Build:  %v (%0.1f%%)\n", avgQueryBuild, float64(queryBuildTime)/float64(totalTime)*100)
	fmt.Printf("3. DB Execution: %v (%0.1f%%)\n", avgDbExec, float64(dbExecTime)/float64(totalTime)*100)
	fmt.Printf("4. Scan & Set:   %v (%0.1f%%)\n", avgScanSet, float64(scanSetTime)/float64(totalTime)*100)
	fmt.Printf("5. Total:        %v\n", avgTotal)
}

func BenchmarkFindOneMemProfile(b *testing.B) {
	f, err := os.Create("mem.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	user := &User{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.FindOne(user)
	}

	pprof.WriteHeapProfile(f)
}

//func BenchmarkPGXRawScan(b *testing.B) {
//	ctx := context.Background()
//	pool, err := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer pool.Close()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		var id int
//		//var username, email string
//		err := pool.QueryRow(ctx, `SELECT id FROM users LIMIT 1`).Scan(&id)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkBareQuery(b *testing.B) {
//	ctx := context.Background()
//	pool, _ := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
//	defer pool.Close()
//
//	query := `SELECT 1`
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		var x int
//		_ = pool.QueryRow(ctx, query).Scan(&x)
//	}
//}
