package engine

import (
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/schema"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
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
			MaxOpen:     5,
			MaxIdle:     2,
			MaxLifetime: 5 * time.Minute,
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

// Benchmark for Find operations with 100 rows - broken down by step
func BenchmarkFind_Step1_ReflectValueOf(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var users []*User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Just the initial reflection
		destVal := reflect.ValueOf(&users)
		if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
			b.Fatal("wrong type")
		}
		destType := destVal.Type().Elem().Elem()
		_ = destType
	}
}

func BenchmarkFind_Step2_Introspect(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var users []*User
	destVal := reflect.ValueOf(&users)
	destType := destVal.Type().Elem().Elem()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := e.schema.Introspect(destType)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_Step3_BuildQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	var users []*User
	destType := reflect.ValueOf(&users).Type().Elem().Elem()
	meta, _ := e.schema.Introspect(destType)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		e.Limit(100)
		_, _, err := e.Builder.Build(meta.TableName, meta.Columns)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_Step4_DatabaseQuery(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(100)
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

func BenchmarkFind_Step5_MakeSlices(b *testing.B) {
	colCount := 7

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vals := make([]any, colCount)
		ptrs := make([]any, colCount)
		for j := range ptrs {
			ptrs[j] = &vals[j]
		}
		_ = vals
		_ = ptrs
	}
}

func BenchmarkFind_Step6_ReflectNew_SingleStruct(b *testing.B) {
	var users []*User
	destType := reflect.ValueOf(&users).Type().Elem().Elem()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		destPtr := reflect.New(destType.Elem())
		_ = destPtr
	}
}

func BenchmarkFind_Step7_ReflectNew_100Structs(b *testing.B) {
	var users []*User
	destType := reflect.ValueOf(&users).Type().Elem().Elem()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate creating 100 structs
		for j := 0; j < 100; j++ {
			destPtr := reflect.New(destType.Elem())
			_ = destPtr
		}
	}
}

func BenchmarkFind_Step8_RowsScan_SingleRow(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, _ := e.db.Query(query, args...)

		vals := make([]any, 7)
		ptrs := make([]any, 7)
		for j := range vals {
			ptrs[j] = &vals[j]
		}

		if rows.Next() {
			rows.Scan(ptrs...)
		}
		rows.Close()
	}
}

func BenchmarkFind_Step9_ScanAndSet_SingleStruct(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	cols := meta.Columns
	vals := []any{int64(1), "John", "john@example.com", time.Now(), time.Now(), int32(100), int64(1000)}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := &User{}
		err := meta.ScanAndSet(user, cols, vals)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_Step10_ReflectAppend_SingleItem(b *testing.B) {
	var users []*User
	user := &User{ID: 1, FirstName: "Test"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset slice
		users = users[:0]

		sliceVal := reflect.ValueOf(&users).Elem()
		userVal := reflect.ValueOf(user)
		sliceVal.Set(reflect.Append(sliceVal, userVal))
	}
}

func BenchmarkFind_Step11_ReflectAppend_100Items(b *testing.B) {
	var users []*User
	user := &User{ID: 1, FirstName: "Test"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset slice
		users = users[:0]
		sliceVal := reflect.ValueOf(&users).Elem()

		// Append 100 items
		for j := 0; j < 100; j++ {
			userVal := reflect.ValueOf(user)
			sliceVal.Set(reflect.Append(sliceVal, userVal))
		}
	}
}

func BenchmarkFind_Step12_CompleteRowProcessing(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(&User{}))
	e.Limit(1)
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	var users []*User
	destType := reflect.ValueOf(&users).Type().Elem().Elem()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rows, _ := e.db.Query(query, args...)

		vals := make([]any, 7)
		ptrs := make([]any, 7)
		for j := range vals {
			ptrs[j] = &vals[j]
		}

		if rows.Next() {
			// Create struct
			destPtr := reflect.New(destType.Elem())

			// Scan
			rows.Scan(ptrs...)

			// Set fields
			meta.ScanAndSet(destPtr.Interface(), meta.Columns, vals)

			// Append
			sliceVal := reflect.ValueOf(&users).Elem()
			sliceVal.Set(reflect.Append(sliceVal, destPtr))
		}
		rows.Close()

		// Reset slice
		users = users[:0]
	}
}

func BenchmarkFind_Step13_ReflectValueOfInLoop(b *testing.B) {
	var users []*User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate calling reflect.ValueOf(dest).Elem() 100 times
		for j := 0; j < 100; j++ {
			sliceVal := reflect.ValueOf(&users).Elem()
			_ = sliceVal
		}
	}
}

func BenchmarkFind_Comparison_DirectAppend(b *testing.B) {
	var users []*User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users = users[:0]

		// Direct append without reflection
		for j := 0; j < 100; j++ {
			user := &User{ID: uint64(j), FirstName: "Test"}
			users = append(users, user)
		}
	}
}

func BenchmarkFind_Comparison_ReflectAppend(b *testing.B) {
	var users []*User

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users = users[:0]
		sliceVal := reflect.ValueOf(&users).Elem()

		// Reflect append
		for j := 0; j < 100; j++ {
			user := &User{ID: uint64(j), FirstName: "Test"}
			sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(user)))
		}
	}
}

// Full simulation of Find with 100 rows
func BenchmarkFind_FullSimulation_100Rows(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	// Prepare 100 rows of mock data
	mockVals := make([][]any, 100)
	for i := range mockVals {
		mockVals[i] = []any{
			int64(i),
			"John",
			"john@example.com",
			time.Now(),
			time.Now(),
			int32(100),
			int64(1000),
		}
	}

	var users []*User
	destVal := reflect.ValueOf(&users)
	destType := destVal.Type().Elem().Elem()
	meta, _ := e.schema.Introspect(destType)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset slice
		users = users[:0]

		// Make scan arrays
		vals := make([]any, 7)
		ptrs := make([]any, 7)
		for j := range ptrs {
			ptrs[j] = &vals[j]
		}

		// Process 100 rows
		for rowIdx := 0; rowIdx < 100; rowIdx++ {
			// Step 1: Create new struct
			destPtr := reflect.New(destType.Elem())

			// Step 2: Simulate scan (copy mock data)
			for j := 0; j < 7; j++ {
				vals[j] = mockVals[rowIdx][j]
			}

			// Step 3: ScanAndSet
			meta.ScanAndSet(destPtr.Interface(), meta.Columns, vals)

			// Step 4: Append with reflection
			sliceVal := reflect.ValueOf(&users).Elem()
			sliceVal.Set(reflect.Append(sliceVal, destPtr))
		}
	}
}

// Benchmark slice growing pattern
func BenchmarkFind_SliceGrowing(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []*User
		sliceVal := reflect.ValueOf(&users).Elem()

		// Simulate growing from 0 to 100
		for j := 0; j < 100; j++ {
			user := &User{ID: uint64(j)}
			sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(user)))
		}
	}
}

// Benchmark with pre-allocated slice
func BenchmarkFind_PreAllocatedSlice(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users := make([]*User, 0, 100) // Pre-allocate capacity
		sliceVal := reflect.ValueOf(&users).Elem()

		for j := 0; j < 100; j++ {
			user := &User{ID: uint64(j)}
			sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(user)))
		}
	}
}

func BenchmarkFind_PreAllocatedStructs(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	meta, _ := e.schema.Introspect(reflect.TypeOf(User{}))
	query, args, _ := e.Builder.Build(meta.TableName, meta.Columns)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Pre-allocate all structs like pgx
		users := make([]User, 100)

		rows, _ := e.db.Query(query, args...)

		j := 0
		for rows.Next() && j < 100 {
			err := rows.Scan(
				&users[j].ID,
				&users[j].FirstName,
				&users[j].Email,
				&users[j].CreatedAt,
				&users[j].UpdatedAt,
				&users[j].Likes,
				&users[j].Counter,
			)
			if err != nil {
				b.Fatal(err)
			}
			j++
		}
		rows.Close()
	}
}

func BenchmarkFind_ReformStyle(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	// Prepare statement once
	query := "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100"
	stmt, _ := e.db.Prepare(query)
	defer stmt.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users := make([]*User, 0, 100)

		rows, _ := stmt.Query()

		for rows.Next() {
			u := &User{}
			rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
			users = append(users, u)
		}
		rows.Close()
	}
}
func BenchmarkFind_PgxPrepared(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	// With pgx, you don't need explicit Prepare
	// Just use named queries or query caching
	query := "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users := make([]*User, 0, 100)

		// pgxpool automatically prepares frequently used queries
		rows, _ := e.db.Query(query)

		for rows.Next() {
			u := &User{}
			rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
			users = append(users, u)
		}
		rows.Close()
	}
}

var userPool = sync.Pool{
	New: func() interface{} {
		return &User{}
	},
}

func BenchmarkFind_WithStructPool(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	query := "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users := make([]*User, 0, 100)

		rows, _ := e.db.Query(query)

		for rows.Next() {
			u := userPool.Get().(*User)
			// Reset fields if needed
			*u = User{} // Clear previous values

			rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
			users = append(users, u)
		}
		rows.Close()

		// Return to pool after use (in real code, caller would do this)
		for _, u := range users {
			userPool.Put(u)
		}
	}
}

func BenchmarkFind_StringAllocations(b *testing.B) {
	var testStrings []string

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate scanning 100 rows with 2 strings each
		for j := 0; j < 100; j++ {
			firstName := "John"         // This doesn't allocate - string literal
			email := "john@example.com" // This doesn't allocate either
			testStrings = append(testStrings, firstName, email)
		}
		testStrings = testStrings[:0]
	}
}

func BenchmarkFind_StringAllocationsWithConversion(b *testing.B) {
	var testStrings []string

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate what happens during actual scanning
		for j := 0; j < 100; j++ {
			// Database returns []byte, converted to string
			firstNameBytes := []byte("John")
			emailBytes := []byte("john@example.com")

			firstName := string(firstNameBytes) // Allocates!
			email := string(emailBytes)         // Allocates!
			testStrings = append(testStrings, firstName, email)
		}
		testStrings = testStrings[:0]
	}
}

// ------------------------------------------------------------
// -----------------------------------------------------------
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

func BenchmarkFind(b *testing.B) {
	e := createTestEngine()
	defer e.Close()

	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
	})

	var u []*User

	// Warm-up or validate connection
	_, _ = e.Limit(100).Find(&u)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Limit(100).Find(&u)
	}

	b.ReportAllocs()

}

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
