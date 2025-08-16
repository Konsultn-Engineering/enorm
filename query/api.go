package query

type HasMany[T any] struct {
}

type HasManyE[T any] []T

type User struct {
	Posts HasManyE[Post]
}

type Post struct {
	Author *User
}

type Team struct {
	Owner *User
}

func main() {
	//enorm := enorm.Connect('pg', ConnectionConfig{
	//
	//})
	//enorm.Select(cols).WhereEq('field', val).Where('expr', args...).

}
