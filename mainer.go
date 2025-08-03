package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/query"
	"github.com/Konsultn-Engineering/enorm/schema"
	"time"
)

type Users struct {
	ID        uint64 `db:"column:public_id;primary;type:string;generator:snowflake"`
	FirstName string `db:"not null"`
	Email     string `db:"column:email_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

func main() {
	selectB := query.Select[Users](schema.New())
	selectB.Columns("public_id", "email_id", "first_name")
	fmt.Println(selectB.ToSQL())
}
