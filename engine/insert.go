package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
	"strings"
)

func (s *Engine) Create(input any) error {
	val := reflect.ValueOf(input)
	if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct {
		return s.insertStruct(val.Elem())
	}
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			if err := s.insertStruct(elem); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("Create expects *T or []*T")
}

func (s *Engine) insertStruct(v reflect.Value) error {
	meta, err := schema.GetModelMeta(v.Type())
	if err != nil {
		return err
	}

	cols, vals, placeholders, idFieldAddr, err := buildInsert(meta, v)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		meta.TableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	if idFieldAddr != nil {
		query += " RETURNING id"
		return s.db.QueryRow(query, vals...).Scan(idFieldAddr)
	}

	_, err = s.db.Exec(query, vals...)
	return err
}
