package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/schema"
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

	schema.New(2000, nil)

	meta, _ := schema.Introspect(reflect.TypeOf(u))
	fmt.Println("Entity Name:", meta.Name)
	fmt.Println("Fields:", meta.Fields)
	for _, field := range meta.Fields {
		fmt.Printf("Field: %s, DB Name: %s, Type: %s\n", field.Name, field.DBName, field.Type)
		if field.Generator != nil {
			fmt.Println(field.Generator.Generate())
		}
	}

	converter, err := schema.GetConverter[uint64](reflect.TypeOf(""), reflect.TypeOf(uint64(123)))
	if err != nil {
	}
	converted, _ := converter("123")
	fmt.Println("Converter for string to uint64:", converted)
}
