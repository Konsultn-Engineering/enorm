package main

import (
	"context"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/engine"
	_ "github.com/Konsultn-Engineering/enorm/providers/postgres"
	"time"
)

type Users struct {
	ID        uint64 `db:"column:public_id;primary;type:string;generator:snowflake"`
	FirstName string `db:"not null"`
	Email     string `db:"column:email_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type User struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
	Likes     int
	Counter   uint64
}

type Post struct {
	ID      uint64
	Title   string
	Content string
}

func main() {
	enorm, err := connector.New("postgres", connector.Config{
		Host:           "localhost",
		Port:           5432,
		Database:       "enorm_test",
		Username:       "postgres",
		Password:       "admin",
		SSLMode:        "disable",
		ConnectTimeout: 0,
		QueryTimeout:   0,
	})

	if err != nil {
		panic(err)
	}

	en, err := enorm.Connect(context.Background())
	if err != nil {
		panic(err)
	}

	eng := engine.New(en.DB())

	user := &User{}

	query, err := eng.FindOne(user)

	fmt.Println(query)
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
}
