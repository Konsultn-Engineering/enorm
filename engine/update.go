package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
	"strings"
)

func (e *Engine) Update(model any, updates map[string]any) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("Update expects struct or pointer to struct")
	}

	meta, err := schema.GetModelMeta(val.Type())
	if err != nil {
		return err
	}

	idCol, idVal, err := getID(meta, val)
	if err != nil {
		return err
	}

	skip := map[string]struct{}{
		"ID":        {},
		"CreatedAt": {},
	}

	setClause, args, err := buildUpdateSet(meta, updates, skip)

	if err != nil {
		return err
	}

	if len(setClause) == 0 {
		return fmt.Errorf("no updatable fields")
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = $%d",
		meta.TableName,
		strings.Join(setClause, ", "),
		idCol,
		len(args)+1,
	)
	args = append(args, idVal)

	_, err = e.db.Exec(query, args...)
	return err
}
