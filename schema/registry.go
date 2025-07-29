package schema

import (
	"fmt"
	"reflect"
)

type fieldRegistry struct {
	entity any
	binds  map[string]func(entity any, val any)
}

func newRegistry(entity any) *fieldRegistry {
	return &fieldRegistry{
		entity: entity,
		binds:  map[string]func(entity any, val any){},
	}
}

func (f *fieldRegistry) Bind(entity any, fields ...any) error {
	structVal := reflect.ValueOf(entity)
	if structVal.Kind() != reflect.Ptr {
		return fmt.Errorf("bind target must be a pointer to struct")
	}
	structVal = structVal.Elem()
	structType := structVal.Type()

	meta, err := Introspect(structType)
	if err != nil {
		return err
	}

	for _, fieldPtr := range fields {
		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr {
			return fmt.Errorf("bind field must be pointer")
		}

		found := false
		for i := 0; i < structVal.NumField(); i++ {
			field := structVal.Field(i)
			if field.CanAddr() && field.Addr().Interface() == fieldPtr {
				fieldName := structType.Field(i).Name
				dbName := formatName(fieldName)

				if fm, ok := meta.FieldMap[fieldName]; ok && fm.SetFast != nil {
					f.binds[dbName] = fm.SetFast
				} else {
					return fmt.Errorf("no SetFast for field %s", fieldName)
				}

				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("bind field not found in struct: maybe not a field of the supplied structPtr")
		}
	}
	return nil
}

func (f *fieldRegistry) GetBinds() map[string]func(model any, val any) {
	return f.binds
}
