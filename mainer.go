package main

import (
	"fmt"
	"reflect"
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
	u := &Users{}
	structType := reflect.TypeOf(*u)
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fmt.Printf("Field %d: %s, Type: %s, Tag: %s\n", i, field.Name, field.Type, field.Tag)
		fmt.Println("new ref", reflect.TypeOf(reflect.New(field.Type).Elem().Interface()))
	}
	fmt.Println(structType)
}
