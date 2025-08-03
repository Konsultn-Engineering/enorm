package schema

import (
	"fmt"
	"reflect"
	"unsafe"
)

func createDirectSetterForType(fieldType reflect.Type, offset uintptr) SetterFunc {
	if creator, ok := setterCreators.Load(fieldType); ok {
		return creator.(func(uintptr) func(unsafe.Pointer, any))(offset)
	}

	// Try again after registration
	if creator, ok := setterCreators.Load(fieldType); ok {
		return creator.(func(uintptr) func(unsafe.Pointer, any))(offset)
	}

	// Reflection fallback
	return func(structPtr unsafe.Pointer, value any) {
		fieldPtr := unsafe.Add(structPtr, offset)
		targetValue := reflect.NewAt(fieldType, fieldPtr).Elem()

		if value == nil {
			targetValue.Set(reflect.Zero(fieldType))
			return
		}

		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Ptr && !val.IsNil() {
			val = val.Elem()
		}

		if val.Type() == fieldType {
			targetValue.Set(val)
		} else if val.Type().ConvertibleTo(fieldType) {
			targetValue.Set(val.Convert(fieldType))
		} else {
			panic(fmt.Sprintf("cannot set field of type %s with value of type %s", fieldType, val.Type()))
		}
	}
}
