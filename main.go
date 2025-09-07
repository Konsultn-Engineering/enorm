package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/database"
	"github.com/Konsultn-Engineering/enorm/engine"
)

type Model struct {
	ID      int `db:"primary;auto"`
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

	newModel := Model{Name: "Test", Title: "Title", Fax: "123", Web: "example.com", Right: true, Age: 30, Counter: 100}

	_, err := e.Save(&newModel)

	if err != nil {
		panic("Failed to save: " + err.Error())
	}

	fmt.Println(newModel)
}
