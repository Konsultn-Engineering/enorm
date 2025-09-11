package engine

import (
	"database/sql"
	"github.com/Konsultn-Engineering/enorm/connector"
	"reflect"
	"unsafe"
)

type Genorm[T any] struct {
	*Engine
}

func NewGenorm[T any](conn connector.Connection) *Genorm[T] {
	return &Genorm[T]{
		NewEngine(conn),
	}
}

func (e *Genorm[T]) FindOne(dest *T) (string, error) {
	e.Limit(1)

	meta, err := e.schema.Introspect(reflect.TypeOf(*dest))
	if err != nil {
		return "", err
	}

	queryStr, args, err := e.Builder.Build(meta.TableName, meta.Columns)
	if err != nil {
		return "", err
	}

	rows, err := e.db.Query(queryStr, args...)
	if err != nil {
		return queryStr, err
	}
	defer rows.Close()

	if !rows.Next() {
		return queryStr, sql.ErrNoRows
	}

	structPtr := unsafe.Pointer(dest)
	scanPtrs := e.setupScanPointers(meta, structPtr)

	if err = rows.Scan(scanPtrs...); err != nil {
		return queryStr, err
	}

	return queryStr, nil
}
