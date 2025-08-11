package main

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	_ "github.com/Konsultn-Engineering/enorm/providers/postgres"
	"github.com/Konsultn-Engineering/enorm/query"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"time"
)

type Users struct {
	ID        uint64 `db:"column:public_id;primary;type:string;generator:snowflake"`
	FirstName string `db:"not null"`
	Email     string `db:"column:email_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
	Posts     HasMany[Post]
}

type Post struct {
	ID      uint64
	Title   string
	Content string
}

func main() {
	v := visitor.NewSQLVisitor(dialect.NewPostgresDialect(), cache.NewQueryCache())
	selectBuilder := query.NewSelectBuilder("", "table", v).WhereEq("column", "value").
		WhereEq("abc", "def").
		WhereIn("columns", []any{"value1", "value2"}).
		WhereIsNull("column2").
		WhereExists(func(sb *query.SelectBuilder) {
			sb.WhereEq("column3", "value3")
		}).
		WhereNotExists(func(sb *query.SelectBuilder) {
			sb.WhereEq("column4", "value4")
		}).OrderByAsc("id", "name", "created_at").
		OrderByDesc("updated_at", "another_field").
		OrderByAsc("somefield").
		InnerJoin("table").On("field", "field").
		Limit(10).LimitOffset(10, 1).Offset(20)

	fmt.Println(selectBuilder.Build())
	selectBuilder.Release()
}
