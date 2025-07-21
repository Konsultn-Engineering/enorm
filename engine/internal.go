package engine

import "reflect"

func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int64, reflect.Int32:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint64, reflect.Uint32:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Struct:
		z := reflect.Zero(v.Type())
		return reflect.DeepEqual(v.Interface(), z.Interface())
	default:
		return false
	}
}

func followPath(v reflect.Value, path []int) reflect.Value {
	for _, i := range path {
		v = v.Field(i)
		if v.Kind() == reflect.Ptr && !v.IsNil() {
			v = v.Elem()
		}
	}
	return v
}
