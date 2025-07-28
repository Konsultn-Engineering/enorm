package schema

import (
	"fmt"
	"reflect"
)

// EntityMeta holds introspected metadata about a struct.
type EntityMeta struct {
	Type         reflect.Type
	Name         string
	Plural       string
	Fields       []*FieldMeta
	FieldMap     map[string]*FieldMeta
	SnakeMap     map[string]*FieldMeta
	ScannerFn    ScannerFunc
	AliasMapping map[string]string
}

// FieldMeta holds metadata about a struct field.
type FieldMeta struct {
	GoName     string
	DBName     string
	Type       reflect.Type
	Index      []int // for FieldByIndex
	Tag        reflect.StructTag
	IsExported bool
}

// ScannerFunc abstracts a scanner hook (if implemented).
type ScannerFunc func(any, RowScanner) error

// RowScanner = like sql.Rows or pgx.Row
type RowScanner interface {
	Scan(dest ...any) error
	Columns() ([]string, error)
}

func buildMeta(t reflect.Type) (*EntityMeta, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s", t.Kind())
	}

	meta := &EntityMeta{
		Type:         t,
		Name:         t.Name(),
		Plural:       pluralize(t.Name()),
		Fields:       []*FieldMeta{},
		FieldMap:     map[string]*FieldMeta{},
		SnakeMap:     map[string]*FieldMeta{},
		AliasMapping: map[string]string{},
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() || f.Anonymous {
			continue
		}

		name := f.Name
		dbName := formatName(name)

		fm := &FieldMeta{
			GoName:     name,
			DBName:     dbName,
			Type:       f.Type,
			Index:      f.Index,
			Tag:        f.Tag,
			IsExported: true,
		}

		meta.Fields = append(meta.Fields, fm)
		meta.FieldMap[name] = fm
		meta.SnakeMap[dbName] = fm
		meta.AliasMapping[dbName] = name
	}

	if fn := getRegisteredScanner(t); fn != nil {
		meta.ScannerFn = fn
	}
	return meta, nil
}
