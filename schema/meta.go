package schema

import (
	"fmt"
	"reflect"
)

// Pre-compiled setter functions to avoid closure allocations
func createSetFunc(index []int, fieldType reflect.Type, fieldName string) func(model any, val any) {
	return func(model any, val any) {
		field := reflect.ValueOf(model).Elem().FieldByIndex(index)
		v := reflect.ValueOf(val)
		if v.Type().ConvertibleTo(fieldType) {
			field.Set(v.Convert(fieldType))
		} else {
			panic(fmt.Sprintf("cannot assign %s to field %s (%s)", v.Type(), fieldName, fieldType))
		}
	}
}

func createSetFastFunc(index []int, fieldType reflect.Type, fieldName string) func(ptr any, raw any) {
	return func(ptr any, raw any) {
		dst := reflect.ValueOf(ptr).Elem().FieldByIndex(index)
		src := reflect.ValueOf(raw)
		if src.Type().ConvertibleTo(fieldType) {
			dst.Set(src.Convert(fieldType))
		} else {
			panic(fmt.Sprintf("type mismatch for field %s", fieldName))
		}
	}
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

		// Pre-compile setter functions to avoid closure allocations
		fm.SetFunc = createSetFunc(f.Index, f.Type, f.Name)
		fm.SetFast = createSetFastFunc(f.Index, f.Type, f.Name)

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
