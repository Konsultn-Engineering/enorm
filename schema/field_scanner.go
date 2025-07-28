package schema

import (
	"fmt"
	"reflect"
)

type FieldScanFunc func(ptr any, val any)

type FieldRegistry interface {
	Bind(entity any, fields ...any) error
	GetBinds() map[string]func(model any, val any)
}

type fieldRegistry struct {
	entity any
	binds  map[string]func(entity any, val any)
}

func (f *fieldRegistry) Bind(entity any, fields ...any) error {
	structVal := reflect.ValueOf(entity)
	if structVal.Kind() != reflect.Ptr {
		return fmt.Errorf("bind target must be a pointer to struct")
	}
	structVal = structVal.Elem()
	structType := structVal.Type()

	for _, fieldPtr := range fields {
		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr {
			return fmt.Errorf("bind field must be pointer")
		}

		// Walk all fields in struct to match pointer identity
		found := false
		for i := 0; i < structVal.NumField(); i++ {
			field := structVal.Field(i)
			if field.CanAddr() && field.Addr().Interface() == fieldPtr {
				fieldName := structType.Field(i).Name
				dbName := formatName(fieldName)
				f.binds[dbName] = func(model any, val any) {
					reflect.ValueOf(model).Elem().FieldByName(fieldName).Set(reflect.ValueOf(val))
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
