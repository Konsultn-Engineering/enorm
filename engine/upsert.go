package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
	"strings"
)

func (e *Engine) Upsert(model any, conflictColumns ...string) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("Upsert expects struct or pointer to struct")
	}

	meta, err := schema.GetModelMeta(val.Type())
	if err != nil {
		return err
	}

	cols, vals, placeholders, _, err := buildInsert(meta, val)
	if err != nil {
		return err
	}

	skip := map[string]struct{}{
		"ID":        {},
		"CreatedAt": {},
	}
	updateSet, _, err := buildUpdateSet(meta, nil, skip)

	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		meta.TableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictColumns, ", "),
		strings.Join(updateSet, ", "),
	)

	_, err = e.db.Exec(query, vals...)
	return err
}
