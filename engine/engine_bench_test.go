package engine

import (
	"context"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/jackc/pgx/v5/pgxpool"
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
	if err != nil {
		panic("Failed to connect: " + err.Error())
	}
	e = New(db.DB())

}

func BenchmarkFindOne(b *testing.B) {
	b.ResetTimer()
	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldRegistry) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt)
	})

	//context := context.Background()

	u := User{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.FindOne(&u) // SELECT * FROM users LIMIT 1
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
		var username, email string
		err := pool.QueryRow(ctx, `SELECT id, first_name, email FROM users WHERE id = 1`).Scan(&id, &username, &email)
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
