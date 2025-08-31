package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/engine"
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
	e, _ := createTestEngine()
	var m []*Model
	e.Find(&m)
	for _, v := range m {
		fmt.Println(v)
	}
}
