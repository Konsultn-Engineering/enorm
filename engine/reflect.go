package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
	"time"
)

// buildInsert extracts column names, values, and placeholders for INSERT.
// It auto-sets CreatedAt and UpdatedAt to time.Now().
// It skips zero-valued ID fields and returns a pointer to the ID for RETURNING.
func buildInsert(meta *schema.ModelMeta, v reflect.Value) (
	cols []string,
	vals []any,
	placeholders []string,
	idFieldAddr any,
	err error,
) {
	now := time.Now()
	i := 1

	for _, f := range meta.Fields {
		field := followPath(v, f.Path)

		if f.Name == "CreatedAt" || f.Name == "UpdatedAt" {
			field.Set(reflect.ValueOf(now))
		}

		if f.Name == "ID" && isZero(field) {
			idFieldAddr = field.Addr().Interface()
			continue
		}

		cols = append(cols, f.Column)
		vals = append(vals, field.Interface())
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}
	return
}

// getID extracts the column name and value of the primary key field named "ID".
// Returns error if ID is missing or zero.
func getID(meta *schema.ModelMeta, v reflect.Value) (col string, val any, err error) {
	for _, f := range meta.Fields {
		if f.Name == "ID" {
			field := followPath(v, f.Path)
			if isZero(field) {
				return "", nil, fmt.Errorf("zero ID value")
			}
			return f.Column, field.Interface(), nil
		}
	}
	return "", nil, fmt.Errorf("ID field not found")
}

// buildUpdateSet maps column keys to struct fields.
// Fails if any key is invalid (no matching column in meta).
func buildUpdateSet(meta *schema.ModelMeta, updates map[string]any, skipCols map[string]struct{}) (
	setClause []string,
	args []any,
	err error,
) {
	i := 1
	now := time.Now()
	fieldMap := make(map[string]schema.FieldMeta)
	for _, f := range meta.Fields {
		fieldMap[f.Column] = f
	}

	for colKey, val := range updates {
		if _, skip := skipCols[colKey]; skip {
			continue
		}
		f, ok := fieldMap[colKey]
		if !ok {
			return nil, nil, fmt.Errorf("unknown column name: %s", colKey)
		}
		if f.Name == "UpdatedAt" {
			continue // auto-added separately
		}
		setClause = append(setClause, fmt.Sprintf("%s = $%d", colKey, i))
		args = append(args, val)
		i++
	}

	// Always update UpdatedAt unless explicitly skipped
	if _, skip := skipCols["UpdatedAt"]; !skip {
		setClause = append(setClause, fmt.Sprintf("updated_at = $%d", i))
		args = append(args, now)
	}

	return setClause, args, nil
}
