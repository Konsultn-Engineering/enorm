package engine

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
	"unsafe"

	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/schema"
)

type User struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
	Likes     int
	Counter   uint64
}

// createTestEngine creates a fresh engine instance for each benchmark
func createTestEngine() *Engine {
	conn, err := connector.New("postgres", connector.Config{
		Host:     "localhost",
		Port:     5432,
		Database: "enorm_test",
		Username: "postgres",
		Password: "admin",
		SSLMode:  "disable",
		Pool: connector.PoolConfig{
			MaxOpen:     50,
			MaxIdle:     20,
			MaxLifetime: 15 * time.Minute,
		},
	})

	if err != nil {
		panic("Failed to init connector: " + err.Error())
	}

	// Create table if not exists
	_, err = conn.DB().Exec(`CREATE TABLE IF NOT EXISTS users (
		id BIGSERIAL PRIMARY KEY,
		first_name TEXT NOT NULL,
		email TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		likes int4 DEFAULT 100,
		counter int8 DEFAULT 1000
	)`)
	if err != nil {
		panic("Failed to create table: " + err.Error())
	}

	// Ensure test data exists
	_, err = conn.DB().Exec(`INSERT INTO users (first_name, email, likes, counter) 
		VALUES ('sol', 'sol@sol.com', 100, 1222) 
		ON CONFLICT DO NOTHING`)
	if err != nil {
		panic("Failed to insert test data: " + err.Error())
	}

	return New(conn)
}

func BenchmarkFindOne(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
	})

	u := User{}

	// Warm-up or validate connection
	_, _ = e.FindOne(&u)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.FindOne(&u)
	}

	b.ReportAllocs()
}

func BenchmarkFindOne_Complete(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var user User
		_, err := e.FindOne(&user)
		if err != nil && err != sql.ErrNoRows {
			b.Fatal(err)
		}
	}
}

func BenchmarkFindOne_SchemaIntrospect(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	destType := reflect.TypeOf(&User{})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := e.schema.Introspect(destType)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFindOne_QueryBuild(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		e.Limit(1)
		_, _, err := e.Builder.Build(meta.TableName, meta.Columns)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFindOne_DatabaseQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query(query, args...)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkFindOne_RowsProcessing(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query(query, args...)
		if err != nil {
			b.Fatal(err)
		}

		if rows.Next() {
			columnNames, err := rows.Columns()
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}

			vals := make([]any, len(columnNames))
			ptrs := make([]any, len(columnNames))
			for j := range vals {
				ptrs[j] = &vals[j]
			}

			err = rows.Scan(ptrs...)
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkFindOne_ScanAndSet(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))

	// Mock scan data - corrected column names and values
	columnNames := []string{"id", "first_name", "email", "created_at", "updated_at", "likes", "counter"}
	vals := []any{int64(1), "John Doe", "john@example.com", time.Now(), time.Now(), int32(100), int64(1000)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var user User
		err := meta.ScanAndSet(&user, columnNames, vals)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFindOne_SliceAllocation(b *testing.B) {
	numCols := 7 // Updated to match actual column count

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := make([]any, numCols)
		ptrs := make([]any, numCols)
		for j := range vals {
			ptrs[j] = &vals[j]
		}
		_ = vals
		_ = ptrs
	}
}

func BenchmarkFindOne_ReflectTypeOf(b *testing.B) {
	var user User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = reflect.TypeOf(&user)
	}
}

func BenchmarkFindOne_UnsafePointer(b *testing.B) {
	var user User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		structVal := reflect.ValueOf(&user).Elem()
		_ = unsafe.Pointer(structVal.UnsafeAddr())
	}
}

func BenchmarkFindOne_RowsNext(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query(query, args...)
		if err != nil {
			b.Fatal(err)
		}

		// JUST rows.Next()
		hasNext := rows.Next()
		_ = hasNext

		rows.Close()
	}
}

func BenchmarkFindOne_RowsScan(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query(query, args...)
		if err != nil {
			b.Fatal(err)
		}

		if rows.Next() {
			// JUST the scan setup and scan
			vals := make([]any, 7)
			ptrs := make([]any, 7)
			for j := range vals {
				ptrs[j] = &vals[j]
			}
			err = rows.Scan(ptrs...)
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}
		}

		rows.Close()
	}
}

func BenchmarkRawPgxQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	query := "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 1"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query(query)
		if err != nil {
			b.Fatal(err)
		}

		if rows.Next() {
			var id int64
			var firstName, email string
			var createdAt, updatedAt time.Time
			var likes int
			var counter int64
			err = rows.Scan(&id, &firstName, &email, &createdAt, &updatedAt, &likes, &counter)
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}
		}

		rows.Close()
	}
}

func BenchmarkFindOneWithProfile(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

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
	e := createTestEngine()
	defer e.Close()

	userType := reflect.TypeOf(User{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.schema.Introspect(userType)
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
	e := createTestEngine()
	defer e.Close()

	user := &User{}
	meta, _ := e.schema.Introspect(reflect.TypeOf(user))
	fieldMeta := meta.ColumnMap["first_name"]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fieldMeta.DirectSet(unsafe.Pointer(user), "John")
	}
}

func BenchmarkDatabaseQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkRowScan(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, err := e.db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
		if err != nil {
			b.Fatal(err)
		}

		if rows.Next() {
			scanVals := make([]any, 5)
			scanPtrs := make([]any, 5)
			for j := range scanVals {
				scanPtrs[j] = &scanVals[j]
			}

			err := rows.Scan(scanPtrs...)
			if err != nil {
				rows.Close()
				b.Fatal(err)
			}
		}
		rows.Close()
	}
}

func BenchmarkFindOneStepByStep(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	user := &User{}

	b.Run("Introspect", func(b *testing.B) {
		userType := reflect.TypeOf(user)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := e.schema.Introspect(userType)
			if err != nil {
				b.Fatal(err)
			}
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
		meta, _ := e.schema.Introspect(reflect.TypeOf(user))
		cols := []string{"id", "first_name", "email", "created_at", "updated_at"}
		scanVals := []any{int64(1), "John", "john@example.com", time.Now(), time.Now()}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			freshUser := &User{}
			err := meta.ScanAndSet(freshUser, cols, scanVals)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFindOneMemProfile(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	f, err := os.Create("mem.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	user := &User{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.FindOne(user)
		if err != nil {
			b.Fatal(err)
		}
	}

	pprof.WriteHeapProfile(f)
}

// Additional comprehensive benchmarks for detailed analysis
func BenchmarkFindOneWithTiming(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

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
		meta, err := e.schema.Introspect(reflect.TypeOf(user))
		if err != nil {
			b.Fatal(err)
		}
		introspectTime += time.Since(introspectStart)

		// 2. Query Building
		queryBuildStart := time.Now()
		e.Limit(1)
		query, args, err := e.Builder.Build(meta.TableName, meta.Columns)
		if err != nil {
			b.Fatal(err)
		}
		queryBuildTime += time.Since(queryBuildStart)

		// 3. Database Execution
		dbExecStart := time.Now()
		rows, err := e.db.Query(query, args...)
		if err != nil {
			b.Fatal(err)
		}

		if !rows.Next() {
			rows.Close()
			continue
		}

		columnNames, err := rows.Columns()
		if err != nil {
			rows.Close()
			b.Fatal(err)
		}

		scanVals := make([]any, len(columnNames))
		scanPtrs := make([]any, len(columnNames))
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
		err = meta.ScanAndSet(user, columnNames, scanVals)
		if err != nil {
			b.Fatal(err)
		}
		scanSetTime += time.Since(scanSetStart)

		totalTime += time.Since(start)
	}

	if b.N > 0 {
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
}

// Comparison with pure pgx (uncomment if you want to test)
/*
func BenchmarkPGXRawScan(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int64
		var firstName, email string
		var createdAt, updatedAt time.Time
		var likes int
		var counter int64
		err := pool.QueryRow(ctx, `SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 1`).Scan(
			&id, &firstName, &email, &createdAt, &updatedAt, &likes, &counter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBareQuery(b *testing.B) {
	ctx := context.Background()
	pool, _ := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
	defer pool.Close()

	query := `SELECT 1`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var x int
		_ = pool.QueryRow(ctx, query).Scan(&x)
	}
}
*/
