package engine

import (
	"context"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/jackc/pgx/v5/pgxpool"
	"runtime"
	"testing"
	"time"

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

func BenchmarkFindOne(b *testing.B) {
	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	u := User{}

	// Warm-up or validate connection
	//_, _ = e.FindOne(&u)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.FindOne(&u)
	}

	b.ReportAllocs()
}

func BenchmarkPGXRawScan(b *testing.B) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id int
		//var username, email string
		err := pool.QueryRow(ctx, `SELECT id FROM users LIMIT 1`).Scan(&id)
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
