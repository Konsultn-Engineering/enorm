package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConvertFunc represents a conversion function from any type to T
type ConvertFunc[T any] func(value any) (T, error)

// Pre-compiled converter cache (built once at startup)
var converterCache = sync.Map{} // map[string]func(any) (any, error)

// Build cache key for converter lookup
func converterKey(destType, sourceType reflect.Type) string {
	return destType.String() + "←" + sourceType.String()
}

// GetConverter returns a cached conversion function for optimal performance
// This replaces the previous implementation with a high-performance cached version
func GetConverter[T any](zeroVal T, sourceType reflect.Type) (ConvertFunc[T], error) {
	destType := reflect.TypeOf(zeroVal)

	// Handle pointer types by unwrapping
	origDestType := destType
	origSourceType := sourceType
	if sourceType.Kind() == reflect.Ptr {
		sourceType = sourceType.Elem()
	}
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
	}

	key := converterKey(origDestType, origSourceType)

	// Fast path: check cache first (99%+ hit rate after warmup)
	if cached, ok := converterCache.Load(key); ok {
		converter := cached.(func(any) (any, error))
		return func(value any) (T, error) {
			result, err := converter(value)
			if err != nil {
				var zero T
				return zero, err
			}
			return result.(T), nil
		}, nil
	}

	// Slow path: build converter once and cache it
	converter, err := buildFastConverter(destType, sourceType)
	if err != nil {
		return nil, err
	}

	converterCache.Store(key, converter)

	return func(value any) (T, error) {
		result, err := converter(value)
		if err != nil {
			var zero T
			return zero, err
		}
		return result.(T), nil
	}, nil
}

// buildFastConverter creates optimized converters for specific type pairs
func buildFastConverter(destType, sourceType reflect.Type) (func(any) (any, error), error) {
	destKind := destType.Kind()
	sourceKind := sourceType.Kind()

	// Direct assignment (same types) - ultra-fast path
	if sourceType == destType {
		return func(value any) (any, error) {
			if value == nil {
				return reflect.Zero(destType).Interface(), nil
			}
			return value, nil
		}, nil
	}

	// Build type-specific converters
	switch destKind {
	case reflect.String:
		return buildStringConverter(sourceType, sourceKind), nil
	case reflect.Uint64:
		return buildUint64Converter(sourceType, sourceKind), nil
	case reflect.Int64:
		return buildInt64Converter(sourceType, sourceKind), nil
	case reflect.Float64:
		return buildFloat64Converter(sourceType, sourceKind), nil
	case reflect.Bool:
		return buildBoolConverter(sourceType, sourceKind), nil
	case reflect.Struct:
		if destType.String() == "time.Time" {
			return buildTimeConverter(sourceType, sourceKind), nil
		}
	case reflect.Slice:
		return buildSliceConverter(destType, sourceType, sourceKind)
	case reflect.Map:
		return buildMapConverter(destType, sourceType, sourceKind), nil
	case reflect.Interface:
		return buildInterfaceConverter(destType, sourceType), nil
	default:
		panic(fmt.Errorf("unsupported source type %s conversion, no available converter", sourceType.String()))
	}

	return nil, fmt.Errorf("unsupported conversion from %s to %s", sourceType.String(), destType.String())
}

// ===================
// STRING CONVERTERS
// ===================
func buildStringConverter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return value.(string), nil
		}
	case reflect.Uint64:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatUint(value.(uint64), 10), nil
		}
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatInt(value.(int64), 10), nil
		}
	case reflect.Int32:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatInt(int64(value.(int32)), 10), nil
		}
	case reflect.Int:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.Itoa(value.(int)), nil
		}
	case reflect.Float64:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatFloat(value.(float64), 'f', -1, 64), nil
		}
	case reflect.Float32:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatFloat(float64(value.(float32)), 'f', -1, 32), nil
		}
	case reflect.Bool:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return strconv.FormatBool(value.(bool)), nil
		}
	case reflect.Slice:
		if sourceType.Elem().Kind() == reflect.Uint8 { // []byte
			return func(value any) (any, error) {
				if value == nil {
					return "", nil
				}
				return string(value.([]byte)), nil
			}
		}
		// Handle other slices as JSON
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			if b, err := json.Marshal(value); err == nil {
				return string(b), nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	case reflect.Struct:
		if sourceType.String() == "time.Time" {
			return func(value any) (any, error) {
				if value == nil {
					return "", nil
				}
				return value.(time.Time).Format(time.RFC3339), nil
			}
		}
		// Handle structs as JSON
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			if b, err := json.Marshal(value); err == nil {
				return string(b), nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	case reflect.Map, reflect.Array:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			if b, err := json.Marshal(value); err == nil {
				return string(b), nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	default:
		return func(value any) (any, error) {
			if value == nil {
				return "", nil
			}
			return fmt.Sprintf("%v", value), nil
		}
	}
}

// ===================
// UINT64 CONVERTERS
// ===================
func buildUint64Converter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Uint64:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			return value.(uint64), nil
		}
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			v := value.(int64)
			if v < 0 {
				return uint64(0), fmt.Errorf("negative value %d cannot convert to uint64", v)
			}
			return uint64(v), nil
		}
	case reflect.Int32:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			v := value.(int32)
			if v < 0 {
				return uint64(0), fmt.Errorf("negative value %d cannot convert to uint64", v)
			}
			return uint64(v), nil
		}
	case reflect.Int:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			v := value.(int)
			if v < 0 {
				return uint64(0), fmt.Errorf("negative value %d cannot convert to uint64", v)
			}
			return uint64(v), nil
		}
	case reflect.Uint32:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			return uint64(value.(uint32)), nil
		}
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			v := value.(string)
			if v == "" || v == "NULL" || v == "null" {
				return uint64(0), nil
			}
			parsed, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return uint64(0), err
			}
			return parsed, nil
		}
	case reflect.Float64:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			v := value.(float64)
			if v < 0 || v != float64(uint64(v)) {
				return uint64(0), fmt.Errorf("cannot convert %f to uint64", v)
			}
			return uint64(v), nil
		}
	case reflect.Bool:
		return func(value any) (any, error) {
			if value == nil {
				return uint64(0), nil
			}
			if value.(bool) {
				return uint64(1), nil
			}
			return uint64(0), nil
		}
	default:
		return func(value any) (any, error) {
			return uint64(0), fmt.Errorf("cannot convert %T to uint64", value)
		}
	}
}

// ===================
// INT64 CONVERTERS
// ===================
func buildInt64Converter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			return value.(int64), nil
		}
	case reflect.Uint64:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			v := value.(uint64)
			if v > 9223372036854775807 { // max int64
				return int64(0), fmt.Errorf("uint64 %d too large for int64", v)
			}
			return int64(v), nil
		}
	case reflect.Int32:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			return int64(value.(int32)), nil
		}
	case reflect.Int:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			return int64(value.(int)), nil
		}
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			v := value.(string)
			if v == "" || v == "NULL" || v == "null" {
				return int64(0), nil
			}
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return int64(0), err
			}
			return parsed, nil
		}
	case reflect.Float64:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			v := value.(float64)
			if v != float64(int64(v)) {
				return int64(0), fmt.Errorf("cannot convert %f to int64: precision loss", v)
			}
			return int64(v), nil
		}
	case reflect.Bool:
		return func(value any) (any, error) {
			if value == nil {
				return int64(0), nil
			}
			if value.(bool) {
				return int64(1), nil
			}
			return int64(0), nil
		}
	default:
		return func(value any) (any, error) {
			return int64(0), fmt.Errorf("cannot convert %T to int64", value)
		}
	}
}

// ===================
// FLOAT64 CONVERTERS
// ===================
func buildFloat64Converter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Float64:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			return value.(float64), nil
		}
	case reflect.Float32:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			return float64(value.(float32)), nil
		}
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			return float64(value.(int64)), nil
		}
	case reflect.Uint64:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			return float64(value.(uint64)), nil
		}
	case reflect.Int32:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			return float64(value.(int32)), nil
		}
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			v := value.(string)
			if v == "" || v == "NULL" || v == "null" {
				return float64(0), nil
			}
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return float64(0), err
			}
			return parsed, nil
		}
	case reflect.Bool:
		return func(value any) (any, error) {
			if value == nil {
				return float64(0), nil
			}
			if value.(bool) {
				return float64(1), nil
			}
			return float64(0), nil
		}
	default:
		return func(value any) (any, error) {
			return float64(0), fmt.Errorf("cannot convert %T to float64", value)
		}
	}
}

// ===================
// BOOL CONVERTERS
// ===================
func buildBoolConverter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Bool:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			return value.(bool), nil
		}
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			v := strings.ToLower(value.(string))
			switch v {
			case "true", "t", "1", "yes", "y":
				return true, nil
			case "false", "f", "0", "no", "n", "", "null":
				return false, nil
			default:
				return false, fmt.Errorf("cannot parse '%s' as bool", v)
			}
		}
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			return value.(int64) != 0, nil
		}
	case reflect.Uint64:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			return value.(uint64) != 0, nil
		}
	case reflect.Int32:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			return value.(int32) != 0, nil
		}
	case reflect.Float64:
		return func(value any) (any, error) {
			if value == nil {
				return false, nil
			}
			return value.(float64) != 0, nil
		}
	default:
		return func(value any) (any, error) {
			return false, fmt.Errorf("cannot convert %T to bool", value)
		}
	}
}

// ===================
// TIME CONVERTERS
// ===================
func buildTimeConverter(sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Struct:
		if sourceType.String() == "time.Time" {
			return func(value any) (any, error) {
				if value == nil {
					return time.Time{}, nil
				}
				return value.(time.Time), nil
			}
		}
	case reflect.String:
		// Pre-compile time formats for better performance
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
			"15:04:05",
		}
		return func(value any) (any, error) {
			if value == nil {
				return time.Time{}, nil
			}
			v := value.(string)
			if v == "" || v == "NULL" || v == "null" {
				return time.Time{}, nil
			}

			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					return t, nil
				}
			}
			return time.Time{}, fmt.Errorf("cannot parse time string: %s", v)
		}
	case reflect.Int64:
		return func(value any) (any, error) {
			if value == nil {
				return time.Time{}, nil
			}
			return time.Unix(value.(int64), 0), nil
		}
	default:
		panic(fmt.Errorf("unsupported source type %s for time conversion", sourceType.String()))
	}

	return func(value any) (any, error) {
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", value)
	}
}

// ===================
// SLICE CONVERTERS
// ===================
func buildSliceConverter(destType, sourceType reflect.Type, sourceKind reflect.Kind) (func(any) (any, error), error) {
	elemType := destType.Elem()

	// []byte conversions
	if elemType.Kind() == reflect.Uint8 {
		switch sourceKind {
		case reflect.String:
			return func(value any) (any, error) {
				if value == nil {
					return []byte{}, nil
				}
				return []byte(value.(string)), nil
			}, nil
		case reflect.Slice:
			if sourceType.Elem().Kind() == reflect.Uint8 {
				return func(value any) (any, error) {
					if value == nil {
						return []byte{}, nil
					}
					return value.([]byte), nil
				}, nil
			}
		default:
			panic(fmt.Errorf("unsupported source type %s for []uint8 conversion", sourceType.String()))
		}
	}

	// []float32 conversions (vectors)
	if elemType.Kind() == reflect.Float32 {
		switch sourceKind {
		case reflect.Slice:
			if sourceType.Elem().Kind() == reflect.Float32 {
				return func(value any) (any, error) {
					if value == nil {
						return []float32{}, nil
					}
					return value.([]float32), nil
				}, nil
			}
			if sourceType.Elem().Kind() == reflect.Float64 {
				return func(value any) (any, error) {
					if value == nil {
						return []float32{}, nil
					}
					v := value.([]float64)
					result := make([]float32, len(v))
					for i, f := range v {
						result[i] = float32(f)
					}
					return result, nil
				}, nil
			}
		case reflect.String:
			return func(value any) (any, error) {
				if value == nil {
					return []float32{}, nil
				}
				var result []float32
				if err := json.Unmarshal([]byte(value.(string)), &result); err == nil {
					return result, nil
				}
				return []float32{}, fmt.Errorf("cannot parse JSON array: %s", value.(string))
			}, nil
		default:
			panic(fmt.Errorf("unsupported source type %s for []float32 conversion", sourceType.String()))
		}
	}

	// []float64 conversions (vectors)
	if elemType.Kind() == reflect.Float64 {
		switch sourceKind {
		case reflect.Slice:
			if sourceType.Elem().Kind() == reflect.Float64 {
				return func(value any) (any, error) {
					if value == nil {
						return []float64{}, nil
					}
					return value.([]float64), nil
				}, nil
			}
			if sourceType.Elem().Kind() == reflect.Float32 {
				return func(value any) (any, error) {
					if value == nil {
						return []float64{}, nil
					}
					v := value.([]float32)
					result := make([]float64, len(v))
					for i, f := range v {
						result[i] = float64(f)
					}
					return result, nil
				}, nil
			}
		case reflect.String:
			return func(value any) (any, error) {
				if value == nil {
					return []float64{}, nil
				}
				var result []float64
				if err := json.Unmarshal([]byte(value.(string)), &result); err == nil {
					return result, nil
				}
				return []float64{}, fmt.Errorf("cannot parse JSON array: %s", value.(string))
			}, nil
		default:
			panic(fmt.Errorf("unsupported source type %s for []float64 conversion", sourceType.String()))
		}
	}

	// []string conversions
	if elemType.Kind() == reflect.String {
		switch sourceKind {
		case reflect.Slice:
			if sourceType.Elem().Kind() == reflect.String {
				return func(value any) (any, error) {
					if value == nil {
						return []string{}, nil
					}
					return value.([]string), nil
				}, nil
			}
		case reflect.String:
			return func(value any) (any, error) {
				if value == nil {
					return []string{}, nil
				}
				v := value.(string)
				var result []string
				if err := json.Unmarshal([]byte(v), &result); err == nil {
					return result, nil
				}
				// Try comma-separated
				parts := strings.Split(v, ",")
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				return parts, nil
			}, nil
		default:
			panic(fmt.Errorf("unsupported source type %s for []string conversion", sourceType.String()))
		}
	}

	return func(value any) (any, error) {
		return reflect.Zero(destType).Interface(), fmt.Errorf("cannot convert %T to %s", value, destType.String())
	}, nil
}

// ===================
// MAP CONVERTERS
// ===================
func buildMapConverter(destType, sourceType reflect.Type, sourceKind reflect.Kind) func(any) (any, error) {
	switch sourceKind {
	case reflect.Map:
		return func(value any) (any, error) {
			if value == nil {
				return reflect.Zero(destType).Interface(), nil
			}
			// Try direct assignment first
			if reflect.TypeOf(value) == destType {
				return value, nil
			}
			return reflect.Zero(destType).Interface(), fmt.Errorf("map type mismatch: %T vs %s", value, destType.String())
		}
	case reflect.String:
		return func(value any) (any, error) {
			if value == nil {
				return reflect.Zero(destType).Interface(), nil
			}
			result := reflect.New(destType).Interface()
			if err := json.Unmarshal([]byte(value.(string)), result); err == nil {
				return reflect.ValueOf(result).Elem().Interface(), nil
			}
			return reflect.Zero(destType).Interface(), fmt.Errorf("cannot parse JSON map: %s", value.(string))
		}
	default:
		return func(value any) (any, error) {
			return reflect.Zero(destType).Interface(), fmt.Errorf("cannot convert %T to map", value)
		}
	}
}

// ===================
// INTERFACE CONVERTERS
// ===================
func buildInterfaceConverter(destType, sourceType reflect.Type) func(any) (any, error) {
	if destType.String() == "interface {}" {
		return func(value any) (any, error) {
			return value, nil
		}
	}

	return func(value any) (any, error) {
		return nil, fmt.Errorf("cannot convert %T to interface", value)
	}
}
