package engine

import (
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/schema"
	"runtime"
	"testing"
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

// createTestEngine creates a fresh engine instance for each benchmark
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
