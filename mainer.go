package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/connector"
	"github.com/Konsultn-Engineering/enorm/engine"
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
	conn, err := connector.New("postgres", connector.Config{
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

	enorm := engine.New(conn)

	var users []*User
	_, err = enorm.Find(&users)

	for _, user := range users {
		fmt.Println(user)
	}
}
