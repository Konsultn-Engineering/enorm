package engine

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/schema"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
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
			MaxOpen: 200,
			MaxIdle: 2,
		},
	})

	if err != nil {
		panic("Failed to init connector: " + err.Error())
	}

	// Create table if not exists
	_, err = conn.Database().Exec(`CREATE TABLE IF NOT EXISTS users (
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
	_, err = conn.Database().Exec(`INSERT INTO users (first_name, email, likes, counter) 
		VALUES ('sol', 'sol@sol.com', 100, 1222) 
		ON CONFLICT DO NOTHING`)
	if err != nil {
		panic("Failed to insert test data: " + err.Error())
	}

	return New(conn)
}

// ------------------------------------------------------------
// -----------------------------------------------------------
//func BenchmarkFindOne(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
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

func BenchmarkFind(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	e := createTestEngine()
	defer func(e *Engine) {
		e.Close()
	}(e)

	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
	})

	var u []*User

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Limit(100).Find(&u)
	}

}

func BenchmarkStructCreation(b *testing.B) {
	destType := reflect.TypeOf((*[]User)(nil)).Elem().Elem()

	b.Run("CurrentApproach", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)
			for j := 0; j < 10 && j < destPtrs.Cap(); j++ {
				destPtr := destPtrs.Index(j).Addr()
				structElem := destPtr.Elem()
				structPtr := unsafe.Pointer(structElem.UnsafeAddr())
				_ = structPtr // Use it
			}
		}
	})

	b.Run("DirectAllocation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			structs := make([]User, 10)
			for j := 0; j < 10; j++ {
				structPtr := unsafe.Pointer(&structs[j])
				_ = structPtr // Use it
			}
		}
	})
}

func BenchmarkPointerCreation(b *testing.B) {
	// Test old way
	b.Run("Reflection", func(b *testing.B) {
		structPtr := unsafe.Pointer(&User{})
		fieldType := reflect.TypeOf("")
		offset := uintptr(8) // example offset

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fieldPtr := unsafe.Add(structPtr, offset)
			_ = reflect.NewAt(fieldType, fieldPtr).Interface()
		}
	})

	// Test new way
	b.Run("DirectCast", func(b *testing.B) {
		structPtr := unsafe.Pointer(&User{})
		offset := uintptr(8)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = (*string)(unsafe.Add(structPtr, offset))
		}
	})
}

func BenchmarkFind_Section1_ValidationIntrospection(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var users []*User
	dest := &users

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		destVal := reflect.ValueOf(dest)
		if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
			continue
		}
		destType := destVal.Type().Elem().Elem()
		_, _ = e.schema.Introspect(destType)
	}
	b.ReportAllocs()
}

// Section 2: Query Building (lines 13-17 of Find)
func BenchmarkFind_Section2_QueryBuilding(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	destType := reflect.TypeOf((*User)(nil)).Elem()
	meta, _ := e.schema.Introspect(destType)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = e.Builder.Build(meta.TableName, meta.Columns)
	}
	b.ReportAllocs()
}

// Section 3: Database Query (lines 19-23 of Find)
func BenchmarkFind_Section3_DatabaseQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	destType := reflect.TypeOf((*User)(nil)).Elem()
	meta, _ := e.schema.Introspect(destType)
	queryStr, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := e.db.Query(queryStr, args...)
		rows.Close()
	}
	b.ReportAllocs()
}

// Section 4: Setup & Allocation (lines 25-33 of Find)
func BenchmarkFind_Section4_SetupAllocation(b *testing.B) {
	// Match exactly what your Find function does
	var users []*User
	dest := &users
	destVal := reflect.ValueOf(dest)
	destType := destVal.Type().Elem().Elem() // This gives *User, not User

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		colCount := 7 // User has 7 fields
		results := make([]any, 0, 100)
		destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)
		ptrs := scanPtrPool.Get().([]any)
		ptrs = ptrs[:colCount]

		// Cleanup like in actual code
		for j := range ptrs {
			ptrs[j] = nil
		}
		scanPtrPool.Put(ptrs[:0])

		// Prevent optimization
		_ = results
		_ = destPtrs
	}
	b.ReportAllocs()
}

// Section 5: Field Pointer Creation - THE HOT PATH (lines 35-50 of Find)
func BenchmarkFind_Section5_FieldPointerCreation(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var users []*User
	dest := &users
	destVal := reflect.ValueOf(dest)
	destType := destVal.Type().Elem().Elem() // This gives *User, not User
	meta, _ := e.schema.Introspect(destType)
	destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)
	colCount := len(meta.Columns)
	ptrs := make([]any, colCount)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate 100 rows like your actual code
		for row := 0; row < 100; row++ {
			destPtr := destPtrs.Index(row).Addr()
			structElem := destPtr.Elem()
			structPtr := unsafe.Pointer(structElem.UnsafeAddr())

			for j, col := range meta.Columns {
				fieldMeta := meta.ColumnMap[col]
				fieldPtr := unsafe.Pointer(uintptr(structPtr) + fieldMeta.Offset)
				ptrs[j] = reflect.NewAt(fieldMeta.Type, fieldPtr).Interface()
			}
		}
	}
	b.ReportAllocs()
}

// Section 6: Final Type Conversion (lines 52-57 of Find)
func BenchmarkFind_Section6_TypeConversion(b *testing.B) {
	// Pre-create results like your actual code would have
	results := make([]any, 100)
	for i := 0; i < 100; i++ {
		results[i] = &User{
			ID:        uint64(i),
			FirstName: "test",
			Email:     "test@test.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Likes:     100,
			Counter:   1000,
		}
	}

	var users []*User
	dest := &users
	destVal := reflect.ValueOf(dest)
	sliceType := destVal.Elem().Type()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		typedSlice := reflect.MakeSlice(sliceType, len(results), len(results))
		for j, v := range results {
			typedSlice.Index(j).Set(reflect.ValueOf(v))
		}
		destVal.Elem().Set(typedSlice)
	}
	b.ReportAllocs()
}

// Comparison: Direct scan with no reflection
func BenchmarkFind_DirectScanComparisonX(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := e.db.Query("SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100")

		users := make([]*User, 0, 100)
		for rows.Next() {
			u := &User{}
			_ = rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
			users = append(users, u)
		}
		rows.Close()
		_ = users
	}
	b.ReportAllocs()
}

// Your existing full Find benchmark
func BenchmarkFind_Full(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var u []*User

	// Warm-up
	_, _ = e.Limit(100).Find(&u)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Limit(100).Find(&u)
	}
	b.ReportAllocs()
}

// Summary function
func TestRunAllBenchmarksWithSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark summary in short mode")
	}

	benchmarks := []struct {
		name string
		fn   func(*testing.B)
	}{
		{"Section 1: Validation & Introspection", BenchmarkFind_Section1_ValidationIntrospection},
		{"Section 2: Query Building", BenchmarkFind_Section2_QueryBuilding},
		{"Section 3: Database Query", BenchmarkFind_Section3_DatabaseQuery},
		{"Section 4: Setup & Allocation", BenchmarkFind_Section4_SetupAllocation},
		{"Section 5: Field Pointer Creation (HOT PATH)", BenchmarkFind_Section5_FieldPointerCreation},
		{"Section 6: Type Conversion", BenchmarkFind_Section6_TypeConversion},
		{"Direct Scan Comparison (No Reflection)", BenchmarkFind_DirectScanComparisonX},
		{"Full Find Method", BenchmarkFind_Full},
	}

	fmt.Println("\n=== Find Method Breakdown Analysis ===")
	fmt.Printf("%-45s %15s %15s %15s\n", "Section", "ns/op", "B/op", "allocs/op")
	fmt.Println(strings.Repeat("-", 95))

	for _, bm := range benchmarks {
		result := testing.Benchmark(bm.fn)
		fmt.Printf("%-45s %15d %15d %15d\n",
			bm.name,
			result.NsPerOp(),
			result.AllocedBytesPerOp(),
			result.AllocsPerOp())
	}

	fmt.Println(strings.Repeat("-", 95))
	fmt.Println("* Section 5 is your main loop - likely the biggest bottleneck")
	fmt.Println("* Compare 'Full Find Method' vs 'Direct Scan Comparison' to see reflection overhead")
}

//func BenchmarkDatabaseRTT(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		var dummy int
//		rows, err := e.db.Query("SELECT 1")
//
//		rows.Scan(&dummy)
//
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Measure database query with actual data fetch (no reflection)
//func BenchmarkDatabaseQueryWithData(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query("SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		// Just iterate and close - no scanning
//		for rows.Next() {
//		}
//		rows.Close()
//	}
//	b.ReportAllocs()
//}
//
//// Section 1: Validation and Introspection
//func BenchmarkFind_Section1_ValidationAndIntrospection(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	// Pre-register to avoid registration overhead
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		var users []*User
//		dest := &users
//
//		// Section 1 code
//		destVal := reflect.ValueOf(dest)
//		if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
//			b.Fatal("validation failed")
//		}
//
//		destType := destVal.Type().Elem().Elem()
//		_, err := e.schema.Introspect(destType)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Section 2: Query Building
//func BenchmarkFind_Section2_QueryBuildingX(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	// Pre-introspect to isolate query building
//	var users []*User
//	destType := reflect.TypeOf(&users).Elem().Elem()
//	meta, _ := e.schema.Introspect(destType)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _, err := e.Builder.Build(meta.TableName, meta.Columns)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Section 3: Reflection slice creation and pointer building (without DB)
//func BenchmarkFind_Section3_ReflectionSetup(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//	destType := reflect.TypeOf(&users).Elem().Elem()
//	meta, _ := e.schema.Introspect(destType)
//	colCount := len(meta.Columns)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		// Create results slice
//		results := make([]any, 0, 100)
//
//		// Create reflection slice
//		destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)
//
//		// Simulate building pointers for 10 rows (typical case)
//		for j := 0; j < 10; j++ {
//			destPtr := destPtrs.Index(j).Addr()
//
//			// Build scan pointers
//			ptrs := make([]any, colCount)
//			for k, col := range meta.Columns {
//				fieldMeta := meta.ColumnMap[col]
//				ptrs[k] = destPtr.Elem().FieldByIndex(fieldMeta.Index).Addr().Interface()
//			}
//
//			results = append(results, destPtr.Interface())
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Section 3: Full scanning with actual database rows
//func BenchmarkFind_Section3_FullScanning(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//	destType := reflect.TypeOf(&users).Elem().Elem()
//	meta, _ := e.schema.Introspect(destType)
//	colCount := len(meta.Columns)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		rows, _ := e.db.Query("SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 10")
//
//		results := make([]any, 0, 100)
//		destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)
//
//		for j := 0; j < destPtrs.Cap() && rows.Next(); j++ {
//			destPtr := destPtrs.Index(j).Addr()
//
//			ptrs := make([]any, colCount)
//			for k, col := range meta.Columns {
//				fieldMeta := meta.ColumnMap[col]
//				ptrs[k] = destPtr.Elem().FieldByIndex(fieldMeta.Index).Addr().Interface()
//			}
//
//			rows.Scan(ptrs...)
//			results = append(results, destPtr.Interface())
//		}
//		rows.Close()
//	}
//	b.ReportAllocs()
//}
//
//// Section 4: Type conversion
//func BenchmarkFind_Section4_TypeConversion(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	// Setup
//	var users []*User
//	destVal := reflect.ValueOf(&users)
//	sliceType := destVal.Elem().Type()
//
//	// Create mock results (simulate 100 rows)
//	results := make([]any, 100)
//	for i := 0; i < 100; i++ { // Fill all 100 elements
//		u := &User{
//			ID:        uint64(i),
//			FirstName: fmt.Sprintf("User%d", i),
//			Email:     fmt.Sprintf("user%d@example.com", i),
//			CreatedAt: time.Now(),
//			UpdatedAt: time.Now(),
//			Likes:     100 + i,
//			Counter:   uint64(1000 + i),
//		}
//		results[i] = u
//	}
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		typedSlice := reflect.MakeSlice(sliceType, len(results), len(results))
//		for j, v := range results {
//			typedSlice.Index(j).Set(reflect.ValueOf(v).Convert(sliceType.Elem()))
//		}
//		destVal.Elem().Set(typedSlice)
//	}
//	b.ReportAllocs()
//}
//
//// Isolated benchmark for just the inner loop pointer building
//func BenchmarkFind_PointerBuildingPerRow(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//	destType := reflect.TypeOf(&users).Elem().Elem()
//	meta, _ := e.schema.Introspect(destType)
//	colCount := len(meta.Columns)
//
//	destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 1, 1)
//	destPtr := destPtrs.Index(0).Addr()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		ptrs := make([]any, colCount)
//		for j, col := range meta.Columns {
//			fieldMeta := meta.ColumnMap[col]
//			ptrs[j] = destPtr.Elem().FieldByIndex(fieldMeta.Index).Addr().Interface()
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Compare with a non-reflection version to see theoretical best performance
//func BenchmarkFind_DirectScanComparison(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	// Ensure we have data
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		rows, _ := e.db.Query("SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 10")
//
//		users := make([]*User, 0, 10)
//		for rows.Next() {
//			u := &User{}
//			rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//			users = append(users, u)
//		}
//		rows.Close()
//	}
//	b.ReportAllocs()
//}
//
//// Benchmark to measure the overhead of reflect.ValueOf().Convert()
//func BenchmarkFind_ReflectConvertOverhead(b *testing.B) {
//	// Create sample data
//	users := make([]*User, 10)
//	for i := 0; i < 10; i++ {
//		users[i] = &User{ID: uint64(i)}
//	}
//
//	interfaceSlice := make([]any, 10)
//	for i, u := range users {
//		interfaceSlice[i] = u
//	}
//
//	sliceType := reflect.TypeOf([]*User{})
//	elemType := sliceType.Elem()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		for _, v := range interfaceSlice {
//			reflect.ValueOf(v).Convert(elemType)
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Benchmark the complete Find but with varying row counts
//func BenchmarkFind_1Row(b *testing.B) {
//	benchmarkFindWithRowCount(b, 1)
//}
//
//func BenchmarkFind_10Rows(b *testing.B) {
//	benchmarkFindWithRowCount(b, 10)
//}
//
//func BenchmarkFind_50Rows(b *testing.B) {
//	benchmarkFindWithRowCount(b, 50)
//}
//
//func BenchmarkFind_100Rows(b *testing.B) {
//	benchmarkFindWithRowCount(b, 100)
//}
//
//func benchmarkFindWithRowCount(b *testing.B, rowCount int) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//	runtime.GC()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _ = e.Limit(rowCount).Find(&users)
//	}
//	b.ReportAllocs()
//}
//
//// Benchmark to isolate schema.Introspect performance
//func BenchmarkFind_SchemaIntrospectAlone(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//	destType := reflect.TypeOf(&users).Elem().Elem()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := e.schema.Introspect(destType)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//	b.ReportAllocs()
//}
//
//// Helper function to run all benchmarks and print a summary
//func TestRunAllBenchmarksWithSummaryX(t *testing.T) {
//	if testing.Short() {
//		t.Skip("Skipping benchmark summary in short mode")
//	}
//
//	benchmarks := []struct {
//		name string
//		fn   func(*testing.B)
//	}{
//		{"Database RTT", BenchmarkDatabaseRTT},
//		{"Database Query With Data", BenchmarkDatabaseQueryWithData},
//		{"Section 1: Validation & Introspection", BenchmarkFind_Section1_ValidationAndIntrospection},
//		{"Section 2: Query Building", BenchmarkFind_Section2_QueryBuilding},
//		{"Section 3: Reflection Setup", BenchmarkFind_Section3_ReflectionSetup},
//		{"Section 3: Full Scanning", BenchmarkFind_Section3_FullScanning},
//		{"Section 4: Type Conversion", BenchmarkFind_Section4_TypeConversion},
//		{"Pointer Building Per Row", BenchmarkFind_PointerBuildingPerRow},
//		{"Direct Scan (No Reflection)", BenchmarkFind_DirectScanComparison},
//		{"Reflect Convert Overhead", BenchmarkFind_ReflectConvertOverhead},
//		{"Schema Introspect Alone", BenchmarkFind_SchemaIntrospectAlone},
//		{"Full Find", BenchmarkFind},
//	}
//
//	fmt.Println("\n=== Benchmark Summary ===")
//	fmt.Printf("%-40s %15s %15s %15s\n", "Benchmark", "ns/op", "B/op", "allocs/op")
//	fmt.Println(strings.Repeat("-", 90))
//
//	for _, bm := range benchmarks {
//		result := testing.Benchmark(bm.fn)
//		fmt.Printf("%-40s %15d %15d %15d\n",
//			bm.name,
//			result.NsPerOp(),
//			result.AllocedBytesPerOp(),
//			result.AllocsPerOp())
//	}
//}
//
//// Additional helper to create a flame graph compatible output
//func BenchmarkFind_ProfileMode(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
//		u := a.(*User)
//		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
//	})
//
//	var users []*User
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _ = e.Limit(100).Find(&users)
//	}
//}

//
//func BenchmarkFindOne_Complete(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		var user User
//		_, err := e.FindOne(&user)
//		if err != nil && err != sql.ErrNoRows {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkFindOne_SchemaIntrospect(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	destType := reflect.TypeOf(&User{})
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		_, err := e.schema.Introspect(destType)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkFindOne_QueryBuild(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		e.Limit(1)
//		_, _, err := e.Builder.Build(meta.TableName, meta.Columns)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkFindOne_DatabaseQuery(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//	e.Limit(1)
//	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query(query, args...)
//		if err != nil {
//			b.Fatal(err)
//		}
//		rows.Close()
//	}
//}
//
//func BenchmarkFindOne_RowsProcessing(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//	e.Limit(1)
//	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query(query, args...)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		if rows.Next() {
//			columnNames, err := rows.Columns()
//			if err != nil {
//				rows.Close()
//				b.Fatal(err)
//			}
//
//			vals := make([]any, len(columnNames))
//			ptrs := make([]any, len(columnNames))
//			for j := range vals {
//				ptrs[j] = &vals[j]
//			}
//
//			err = rows.Scan(ptrs...)
//			if err != nil {
//				rows.Close()
//				b.Fatal(err)
//			}
//		}
//		rows.Close()
//	}
//}
//
//func BenchmarkFindOne_ScanAndSet(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//
//	// Mock scan data - corrected column names and values
//	columnNames := []string{"id", "first_name", "email", "created_at", "updated_at", "likes", "counter"}
//	vals := []any{int64(1), "John Doe", "john@example.com", time.Now(), time.Now(), int32(100), int64(1000)}
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		var user User
//		err := meta.ScanAndSet(&user, columnNames, vals)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkFindOne_SliceAllocation(b *testing.B) {
//	numCols := 7 // Updated to match actual column count
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		vals := make([]any, numCols)
//		ptrs := make([]any, numCols)
//		for j := range vals {
//			ptrs[j] = &vals[j]
//		}
//		_ = vals
//		_ = ptrs
//	}
//}
//
//func BenchmarkFindOne_ReflectTypeOf(b *testing.B) {
//	var user User
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		_ = reflect.TypeOf(&user)
//	}
//}
//
//func BenchmarkFindOne_UnsafePointer(b *testing.B) {
//	var user User
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		structVal := reflect.ValueOf(&user).Elem()
//		_ = unsafe.Pointer(structVal.UnsafeAddr())
//	}
//}
//
//func BenchmarkFindOne_RowsNext(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//	e.Limit(1)
//	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query(query, args...)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		// JUST rows.Next()
//		hasNext := rows.Next()
//		_ = hasNext
//
//		rows.Close()
//	}
//}
//
//func BenchmarkFindOne_RowsScan(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
//	e.Limit(1)
//	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query(query, args...)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		if rows.Next() {
//			// JUST the scan setup and scan
//			vals := make([]any, 7)
//			ptrs := make([]any, 7)
//			for j := range vals {
//				ptrs[j] = &vals[j]
//			}
//			err = rows.Scan(ptrs...)
//			if err != nil {
//				rows.Close()
//				b.Fatal(err)
//			}
//		}
//
//		rows.Close()
//	}
//}
//
//func BenchmarkRawPgxQuery(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	query := "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 1"
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query(query)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		if rows.Next() {
//			var id int64
//			var firstName, email string
//			var createdAt, updatedAt time.Time
//			var likes int
//			var counter int64
//			err = rows.Scan(&id, &firstName, &email, &createdAt, &updatedAt, &likes, &counter)
//			if err != nil {
//				rows.Close()
//				b.Fatal(err)
//			}
//		}
//
//		rows.Close()
//	}
//}
//
//func BenchmarkFindOneWithProfile(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	// Create CPU profile
//	f, err := os.Create("cpu.prof")
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer f.Close()
//
//	if err := pprof.StartCPUProfile(f); err != nil {
//		b.Fatal(err)
//	}
//	defer pprof.StopCPUProfile()
//
//	user := &User{}
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := e.FindOne(user)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkSchemaIntrospect(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	userType := reflect.TypeOf(User{})
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := e.schema.Introspect(userType)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkGetConverter(b *testing.B) {
//	var zero string
//	sourceType := reflect.TypeOf(int64(0))
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := schema.GetConverter(zero, sourceType)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkDirectSet(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	user := &User{}
//	meta, _ := e.schema.Introspect(reflect.TypeOf(user))
//	fieldMeta := meta.ColumnMap["first_name"]
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		fieldMeta.DirectSet(unsafe.Pointer(user), "John")
//	}
//}
//
//func BenchmarkDatabaseQuery(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
//		if err != nil {
//			b.Fatal(err)
//		}
//		rows.Close()
//	}
//}
//
//func BenchmarkRowScan(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	b.ResetTimer()
//	b.ReportAllocs()
//
//	for i := 0; i < b.N; i++ {
//		rows, err := e.db.Query("SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1")
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		if rows.Next() {
//			scanVals := make([]any, 5)
//			scanPtrs := make([]any, 5)
//			for j := range scanVals {
//				scanPtrs[j] = &scanVals[j]
//			}
//
//			err := rows.Scan(scanPtrs...)
//			if err != nil {
//				rows.Close()
//				b.Fatal(err)
//			}
//		}
//		rows.Close()
//	}
//}
//
//func BenchmarkFindOneStepByStep(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	user := &User{}
//
//	b.Run("Introspect", func(b *testing.B) {
//		userType := reflect.TypeOf(user)
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			_, err := e.schema.Introspect(userType)
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//
//	b.Run("DatabaseExecution", func(b *testing.B) {
//		query := "SELECT id, first_name, email, created_at, updated_at FROM users LIMIT 1"
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			rows, err := e.db.Query(query)
//			if err != nil {
//				b.Fatal(err)
//			}
//			rows.Close()
//		}
//	})
//
//	b.Run("ScanAndSet", func(b *testing.B) {
//		meta, _ := e.schema.Introspect(reflect.TypeOf(user))
//		cols := []string{"id", "first_name", "email", "created_at", "updated_at"}
//		scanVals := []any{int64(1), "John", "john@example.com", time.Now(), time.Now()}
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			freshUser := &User{}
//			err := meta.ScanAndSet(freshUser, cols, scanVals)
//			if err != nil {
//				b.Fatal(err)
//			}
//		}
//	})
//}
//
//func BenchmarkFindOneMemProfile(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	f, err := os.Create("mem.prof")
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer f.Close()
//
//	user := &User{}
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := e.FindOne(user)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//
//	pprof.WriteHeapProfile(f)
//}
//
//// Additional comprehensive benchmarks for detailed analysis
//func BenchmarkFindOneWithTiming(b *testing.B) {
//	e := createTestEngine()
//	defer e.Close()
//
//	user := &User{}
//
//	var (
//		introspectTime time.Duration
//		queryBuildTime time.Duration
//		dbExecTime     time.Duration
//		scanSetTime    time.Duration
//		totalTime      time.Duration
//	)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		start := time.Now()
//
//		// 1. Introspection
//		introspectStart := time.Now()
//		meta, err := e.schema.Introspect(reflect.TypeOf(user))
//		if err != nil {
//			b.Fatal(err)
//		}
//		introspectTime += time.Since(introspectStart)
//
//		// 2. Query Building
//		queryBuildStart := time.Now()
//		e.Limit(1)
//		query, args, err := e.Builder.Build(meta.TableName, meta.Columns)
//		if err != nil {
//			b.Fatal(err)
//		}
//		queryBuildTime += time.Since(queryBuildStart)
//
//		// 3. Database Execution
//		dbExecStart := time.Now()
//		rows, err := e.db.Query(query, args...)
//		if err != nil {
//			b.Fatal(err)
//		}
//
//		if !rows.Next() {
//			rows.Close()
//			continue
//		}
//
//		columnNames, err := rows.Columns()
//		if err != nil {
//			rows.Close()
//			b.Fatal(err)
//		}
//
//		scanVals := make([]any, len(columnNames))
//		scanPtrs := make([]any, len(columnNames))
//		for j := range scanVals {
//			scanPtrs[j] = &scanVals[j]
//		}
//
//		err = rows.Scan(scanPtrs...)
//		rows.Close()
//		if err != nil {
//			b.Fatal(err)
//		}
//		dbExecTime += time.Since(dbExecStart)
//
//		// 4. Scan and Set
//		scanSetStart := time.Now()
//		err = meta.ScanAndSet(user, columnNames, scanVals)
//		if err != nil {
//			b.Fatal(err)
//		}
//		scanSetTime += time.Since(scanSetStart)
//
//		totalTime += time.Since(start)
//	}
//
//	if b.N > 0 {
//		avgIntrospect := introspectTime / time.Duration(b.N)
//		avgQueryBuild := queryBuildTime / time.Duration(b.N)
//		avgDbExec := dbExecTime / time.Duration(b.N)
//		avgScanSet := scanSetTime / time.Duration(b.N)
//		avgTotal := totalTime / time.Duration(b.N)
//
//		fmt.Printf("\nDetailed Timing Breakdown (avg per operation):\n")
//		fmt.Printf("1. Introspect:   %v (%0.1f%%)\n", avgIntrospect, float64(introspectTime)/float64(totalTime)*100)
//		fmt.Printf("2. Query Build:  %v (%0.1f%%)\n", avgQueryBuild, float64(queryBuildTime)/float64(totalTime)*100)
//		fmt.Printf("3. DB Execution: %v (%0.1f%%)\n", avgDbExec, float64(dbExecTime)/float64(totalTime)*100)
//		fmt.Printf("4. Scan & Set:   %v (%0.1f%%)\n", avgScanSet, float64(scanSetTime)/float64(totalTime)*100)
//		fmt.Printf("5. Total:        %v\n", avgTotal)
//	}
//}

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
