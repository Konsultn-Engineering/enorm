package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
	"time"
)

func (e *Engine) Delete(model any, hard bool) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("Delete expects struct or pointer to struct")
	}

	meta, err := schema.GetModelMeta(val.Type())
	if err != nil {
		return err
	}

	idCol, idVal, err := getID(meta, val)
	if err != nil {
		return err
	}

	var hasDeletedAt bool
	for _, f := range meta.Fields {
		if f.Column == "deleted_at" {
			hasDeletedAt = true
			field := followPath(val, f.Path)
			now := time.Now()
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}
				field = field.Elem()
			}
			field.Set(reflect.ValueOf(now))
			break
		}
	}

	if hard || !hasDeletedAt {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", meta.TableName, idCol)
		_, err = e.db.Exec(query, idVal)
		return err
	}

	query := fmt.Sprintf("UPDATE %s SET deleted_at = $1 WHERE %s = $2", meta.TableName, idCol)
	_, err = e.db.Exec(query, time.Now(), idVal)
	return err
}
