package enorm

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/engine"
	"github.com/Konsultn-Engineering/enorm/schema"
	"runtime"
	"testing"
	"time"
)

type User struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt *time.Time
	UpdatedAt *time.Time
	Likes     int
	Counter   uint64
}

func createTestEngine() (*engine.Engine, database.Database) {
	conn, err := connector.New("postgres", connector.Config{
		Host:     "localhost",
		Port:     5432,
		Database: "enorm_test",
		Username: "postgres",
		Password: "admin",
		SSLMode:  "disable",
		Pool: connector.PoolConfig{
			MaxOpen: 200,
			MaxIdle: 10,
		},
	})

	if err != nil {
		panic("Failed to init connector: " + err.Error())
	}

	return engine.New(conn), conn.Database()
}

func main() {
	//ctx := context.Background()
	//pool, err := pgxpool.New(ctx, "postgres://postgres:admin@localhost:5432/enorm_test?sslmode=disable")
	//if err != nil {
	//	panic(err)
	//}
	//defer pool.Close()
	//
	//start := time.Now()
	//for i := 0; i < 1000; i++ {
	//	rows, err := pool.Query(ctx, "SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100")
	//	if err != nil {
	//		panic(err)
	//	}
	//	for rows.Next() {
	//		var id uint64
	//		var fname, email string
	//		var created, updated time.Time
	//		var likes int
	//		var counter uint64
	//		rows.Scan(&id, &fname, &email, &created, &updated, &likes, &counter)
	//	}
	//	rows.Close()
	//}
	//elapsed := time.Since(start)
	//fmt.Printf("Avg time per op: %.2f µs\n", float64(elapsed.Microseconds())/1000)

	e, db := createTestEngine()
	defer e.Close()

	// Ensure table has 100 rows
	_, _ = db.Exec(`TRUNCATE TABLE users`)
	for i := 0; i < 100; i++ {
		_, _ = db.Exec(
			`INSERT INTO users (first_name, email, likes, counter) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), 100, 1000+i)
	}

	var users []*User

	// Warm up (optional)
	for i := 0; i < 10; i++ {
		_, _ = e.Limit(100).Find(&users)
	}

	const N = 10000 // match Reform manual bench
	runtime.GC()
	time.Sleep(100 * time.Millisecond) // allow system to settle

	start := time.Now()
	for i := 0; i < N; i++ {
		_, err := e.Limit(100).Find(&users)
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)
	avg := float64(elapsed.Microseconds()) / float64(N)
	fmt.Printf("ORM Find manual: Avg time per op: %.2f µs (%d ops in %.2fs)\n", avg, N, elapsed.Seconds())
	//runtime.GOMAXPROCS(runtime.NumCPU())
	//fmt.Println("Raw SQL:")
	//fmt.Println(testing.Benchmark(BenchmarkRawSQL).String())
	//fmt.Println("ORM Find:")
	//fmt.Println(testing.Benchmark(BenchmarkFind_RepoStyle).String())
}

func BenchmarkRawSQL(b *testing.B) {
	e, db := createTestEngine()
	defer e.Close()

	// Ensure table has 100 rows
	_, _ = db.Exec(`TRUNCATE TABLE users`)
	for i := 0; i < 100; i++ {
		_, _ = db.Exec(
			`INSERT INTO users (first_name, email, likes, counter) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), 100, 1000+i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, first_name, email, created_at, updated_at, likes, counter FROM users LIMIT 100")
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var u User
			rows.Scan(&u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
		}
		rows.Close()
	}

}

func BenchmarkFind_RepoStyle(b *testing.B) {
	e, db := createTestEngine()
	defer e.Close()

	// Register scanner as in your normal test
	schema.RegisterScanner(User{}, func(a any, scanner schema.FieldBinder, ctx *schema.Context) error {
		u := a.(*User)
		return scanner.Bind(u, &u.ID, &u.FirstName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.Likes, &u.Counter)
	})

	var users []*User

	// Clean the table and insert 100 rows before timing
	_, _ = db.Exec(`TRUNCATE TABLE users`)
	for i := 0; i < 100; i++ {
		_, _ = db.Exec(
			`INSERT INTO users (first_name, email, likes, counter) VALUES ($1, $2, $3, $4)`,
			fmt.Sprintf("User%d", i), fmt.Sprintf("user%d@example.com", i), 100, 1000+i)
	}

	// Warm up connection pool
	for i := 0; i < 10; i++ {
		_, _ = e.Limit(100).Find(&users)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = e.Limit(100).Find(&users)
	}
}

//}
