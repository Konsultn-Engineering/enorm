package schema

import (
	"fmt"
	"reflect"
)

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

	if tn, ok := reflect.New(t).Interface().(TableNamer); ok {
		meta.Plural = tn.TableName()
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

		fm.SetFunc = func(model any, val any) {
			field := reflect.ValueOf(model).Elem().FieldByIndex(f.Index)
			v := reflect.ValueOf(val)
			if v.Type().ConvertibleTo(field.Type()) {
				field.Set(v.Convert(field.Type()))
			} else {
				panic(fmt.Sprintf("cannot assign %s to field %s (%s)", v.Type(), f.Name, field.Type()))
			}
		}

		idx := f.Index
		targetType := f.Type

		fm.SetFast = func(ptr any, raw any) {
			dst := reflect.ValueOf(ptr).Elem().FieldByIndex(idx)
			src := reflect.ValueOf(raw)
			if src.Type().ConvertibleTo(targetType) {
				dst.Set(src.Convert(targetType))
			} else {
				panic(fmt.Sprintf("type mismatch for field %s", f.Name))
			}
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
