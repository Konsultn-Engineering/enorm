package schema

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

var metaCache sync.Map // map[reflect.Type]*ModelMeta

type FieldMeta struct {
	Name   string       // struct field name
	Column string       // DB column name
	Path   []int        // index path for reflect.Value.FieldByIndex
	Type   reflect.Type // actual field type
}

type ModelMeta struct {
	TableName string
	Fields    []FieldMeta
}

func (m *ModelMeta) Columns() []string {
	cols := make([]string, len(m.Fields))
	for i, f := range m.Fields {
		cols[i] = f.Column
	}
	return cols
}

func GetModelMeta(t reflect.Type) (*ModelMeta, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s", t.Kind())
	}
	if cached, ok := metaCache.Load(t); ok {
		return cached.(*ModelMeta), nil
	}

	meta := &ModelMeta{
		TableName: strings.ToLower(t.Name()) + "s", // naive pluralization
	}

	var fields []FieldMeta
	buildFields(t, []int{}, &fields)
	meta.Fields = fields

	metaCache.Store(t, meta)
	return meta, nil
}

func buildFields(t reflect.Type, path []int, out *[]FieldMeta) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("orm")
		if tag == "-" {
			continue
		}
		opts := strings.Split(tag, ",")
		if len(opts) > 1 && opts[1] == "embedded" {
			buildFields(f.Type, append(path, i), out)
			continue
		}
		column := opts[0]
		if column == "" {
			column = toSnake(f.Name)
		}
		*out = append(*out, FieldMeta{
			Name:   f.Name,
			Column: column,
			Path:   append(path, i),
			Type:   f.Type,
		})
	}
}

func ScanInto(meta *ModelMeta, v reflect.Value, rows *sql.Rows) error {
	dest := make([]any, len(meta.Fields))
	for i, f := range meta.Fields {
		field := v
		for _, idx := range f.Path {
			field = field.Field(idx)
			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}
				field = field.Elem()
			}
		}
		dest[i] = field.Addr().Interface()
	}
	return rows.Scan(dest...)
}

var knownInitialism = map[string]struct{}{
	"ID": {}, "URL": {}, "UUID": {},
}

func toSnake(s string) string {
	var b strings.Builder
	runes := []rune(s)
	n := len(runes)

	for i := 0; i < n; {
		j := i
		for j < n && unicode.IsUpper(runes[j]) {
			j++
		}
		upper := string(runes[i:j])
		if _, ok := knownInitialism[upper]; ok {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteString(strings.ToLower(upper))
			i = j
			continue
		}
		if i > 0 && unicode.IsUpper(runes[i]) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(runes[i]))
		i++
	}
	return b.String()
}
