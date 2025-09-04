package engine

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/schema"
	"reflect"
	"runtime"
	"testing"
	"unsafe"
)

type Model struct {
	ID      int
	Name    string
	Title   string
	Fax     string
	Web     string
	Age     int32
	Right   bool
	Counter int64
}

var globalSink interface{} // Package level

func BenchmarkModular(b *testing.B) {
	b.StopTimer() // Stop measuring immediately
	// Setup (NOT measured)
	e, _ := createTestEngine()
	e.Limit(100)
	defer func(e *Engine) {
		e.Close()
	}(e)
	var dest []*Model

	destVal := reflect.ValueOf(&dest) // Fix: need pointer to dest
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
	}

	destType := destVal.Type().Elem().Elem()
	meta, err := e.schema.Introspect(destType)
	if err != nil {

	}

	queryStr, args, err := e.Builder.Build(meta.TableName, meta.Columns)

	rows, err := e.db.Query(queryStr, args...)
	defer rows.Close()

	colCount := len(meta.Columns)
	//results := make([]any, 0, 100)
	destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)

	ptrs := scanPtrPool.Get().([]any)
	ptrs = ptrs[:colCount]
	defer func() {
		for i := range ptrs {
			ptrs[i] = nil
		}
		scanPtrPool.Put(ptrs[:0])
	}()

	b.StartTimer() // Start measuring here

	fieldMetas := make([]*schema.FieldMeta, len(meta.Columns))
	for i, col := range meta.Columns {
		fieldMetas[i] = meta.ColumnMap[col]
	}

	// ONLY THIS LOOP IS MEASURED
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rows, _ = e.db.Query(queryStr, args...)
		b.StartTimer()
		// Process 100 rows per b.N iteration
		for j := 0; j < destPtrs.Cap() && rows.Next(); j++ {
			b.StopTimer()
			destPtr := destPtrs.Index(j).Addr()
			structElem := destPtr.Elem()                         // Measure this across all rows
			structPtr := unsafe.Pointer(structElem.UnsafeAddr()) // ONLY THIS LINE MEASURED
			b.StartTimer()
			for i := range meta.Columns {
				fieldMeta := fieldMetas[i] // ← Array access instead of map lookup
				if fieldMeta != nil {
					globalSink = unsafe.Pointer(uintptr(structPtr) + fieldMeta.Offset)
				} else {
					globalSink = schema.GetGlobalDummy()
				}
			}

			b.StopTimer()

		}

		// Reset rows for next iteration if needed
		if i < b.N-1 {
			// You might need to reset your query here
		}
		b.StopTimer()
		rows.Close() // ❌ MISSING - you need this!
		b.StartTimer()
	}
	b.ReportAllocs()

}
func BenchmarkNoTimerToggling(b *testing.B) {
	// Setup (NOT measured)
	e, _ := createTestEngine()
	e.Limit(1)
	defer func(e *Engine) {
		e.Close()
	}(e)
	var dest []*Model

	destVal := reflect.ValueOf(&dest) // Fix: need pointer to dest
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
	}

	destType := destVal.Type().Elem().Elem()
	meta, err := e.schema.Introspect(destType)
	if err != nil {

	}

	queryStr, args, err := e.Builder.Build(meta.TableName, meta.Columns)

	rows, err := e.db.Query(queryStr, args...)
	defer rows.Close()

	colCount := len(meta.Columns)
	//results := make([]any, 0, 100)
	destPtrs := reflect.MakeSlice(reflect.SliceOf(destType.Elem()), 100, 100)

	ptrs := scanPtrPool.Get().([]any)
	ptrs = ptrs[:colCount]
	defer func() {
		for i := range ptrs {
			ptrs[i] = nil
		}
		scanPtrPool.Put(ptrs[:0])
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, _ := e.db.Query(queryStr, args...)

		for j := 0; j < destPtrs.Cap() && rows.Next(); j++ {
			destPtr := destPtrs.Index(j).Addr()
			structElem := destPtr.Elem()
			structPtr := unsafe.Pointer(structElem.UnsafeAddr())

			for _, col := range meta.Columns {
				fieldMeta := meta.ColumnMap[col]
				if fieldMeta != nil {
					globalSink = fieldMeta.DirectPtr(structPtr)
				} else {
					globalSink = schema.GetGlobalDummy()
				}
			}
		}
		rows.Close()
	}

	b.ReportAllocs()
}

func BenchmarkDirectPtrOnly(b *testing.B) {
	// Setup once
	var model Model
	s := schema.New()
	structPtr := unsafe.Pointer(&model)
	meta, _ := s.Introspect(reflect.TypeOf(model))
	fmt.Printf("meta.Columns length: %d\n", len(meta.Columns))
	fmt.Printf("Struct fields: %d\n", reflect.TypeOf(Model{}).NumField())

	fieldMeta := meta.ColumnMap[meta.Columns[0]] // First column

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		globalSink = fieldMeta.DirectPtr(structPtr) // Should be 0 allocs
	}
}

func BenchmarkDirectPtrBreakdown(b *testing.B) {
	var model Model
	s := schema.New()
	structPtr := unsafe.Pointer(&model)
	meta, _ := s.Introspect(reflect.TypeOf(model))
	fieldMeta := meta.ColumnMap[meta.Columns[0]]

	b.Run("DirectPtr", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			globalSink = fieldMeta.DirectPtr(structPtr)
		}
	})

	// If you can access the internals, test each step:
	b.Run("UnsafePointerArithmetic", func(b *testing.B) {
		b.ReportAllocs()
		offset := fieldMeta.Offset // If available
		for i := 0; i < b.N; i++ {
			globalSink = unsafe.Pointer(uintptr(structPtr) + offset)
		}
	})
}

// // createTestEngine creates a fresh engine instance for each benchmark
func createTestEngine() (*Engine, database.Database) {
	conn, err := connector.New("postgres", connector.Config{
		Host:     "localhost",
		Port:     5432,
		Database: "enorm_test",
		Username: "postgres",
		Password: "admin",
		SSLMode:  "disable",
	})

	if err != nil {
		panic("Failed to init connector: " + err.Error())
	}

	// Create table if not exists
	_, err = conn.Database().Exec(`CREATE TABLE IF NOT EXISTS models (
			id SERIAL NOT NULL,
			name text NOT NULL,
			title text NOT NULL,
			fax text NOT NULL,
			web text NOT NULL,
			age integer NOT NULL,
			"right" boolean NOT NULL,
			counter bigint NOT NULL,
			CONSTRAINT models_pkey PRIMARY KEY (id)
			) WITH (OIDS=FALSE);`)
	if err != nil {
		panic("Failed to create table: " + err.Error())
	}

	return New(conn), conn.Database()
}

// // ------------------------------------------------------------
// // -----------------------------------------------------------
// //func BenchmarkFindOne(b *testing.B) {
// //	e := createTestEngine()
// //	defer e.Close()
// //
// //	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
// //		u := a.(*User)
// //		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
// //	})
// //
// //	u := User{}
// //
// //	// Warm-up or validate connection
// //	_, _ = e.FindOne(&u)
// //	runtime.GC()
// //	b.ResetTimer()
// //	for i := 0; i < b.N; i++ {
// //		_, _ = e.FindOne(&u)
// //	}
// //
// //	b.ReportAllocs()
// //}
func BenchmarkFind(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	e, db := createTestEngine()
	defer func(e *Engine) {
		e.Close()
	}(e)

	_, err := db.Exec("TRUNCATE TABLE models;")

	for i := 0; i < 100; i++ {
		m := &Model{}
		m.Name = "Orm Benchmark"
		m.Title = "Just a Benchmark for fun"
		m.Fax = "99909990"
		m.Web = "http://blog.milkpod29.me"
		m.Age = 100
		m.Right = true
		m.Counter = 1000
		// Adjust the fields and types to match your table and struct
		_, err = db.Exec(
			`INSERT INTO models (name, title, fax, web, age, "right", counter)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			m.Name, m.Title, m.Fax, m.Web, m.Age, m.Right, m.Counter,
		)
		if err != nil {
			panic("Failed to insert test data: " + err.Error())
		}
	}

	schema.RegisterScanner(Model{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
		u := a.(*Model)
		return scanner.Bind(u, &u.ID, &u.Name, &u.Title, &u.Fax, &u.Web, &u.Age, &u.Right, &u.Counter)
	})

	var u []*Model

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Limit(100).Find(&u)
	}

}
