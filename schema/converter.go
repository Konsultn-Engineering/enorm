package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ConvertFunc represents a conversion function from any type to T
type ConvertFunc[T any] func(value any) (T, error)

// GetConverter returns a conversion function for the given source and destination types
func GetConverter[T any](zeroVal T, sourceType reflect.Type) (ConvertFunc[T], error) {
	destType := reflect.TypeOf(zeroVal)

	// Handle pointer types by unwrapping
	if sourceType.Kind() == reflect.Ptr {
		sourceType = sourceType.Elem()
	}
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
	}

	destKind := destType.Kind()
	sourceKind := sourceType.Kind()

	// Direct assignment (same types)
	if sourceType == destType {
		return func(value any) (T, error) {
			if value == nil {
				var zero T
				return zero, nil
			}
			if v, ok := value.(T); ok {
				return v, nil
			}
			var zero T
			return zero, fmt.Errorf("type mismatch: expected %T, got %T", zero, value)
		}, nil
	}

	// Convert based on destination type
	switch destKind {

	// ===================
	// STRING CONVERSIONS
	// ===================
	case reflect.String:
		return func(value any) (T, error) {
			if value == nil {
				return any("").(T), nil
			}

			switch sourceKind {
			case reflect.String:
				if v, ok := value.(string); ok {
					return any(v).(T), nil
				}
			case reflect.Uint64:
				if v, ok := value.(uint64); ok {
					return any(strconv.FormatUint(v, 10)).(T), nil
				}
			case reflect.Int64:
				if v, ok := value.(int64); ok {
					return any(strconv.FormatInt(v, 10)).(T), nil
				}
			case reflect.Int32:
				if v, ok := value.(int32); ok {
					return any(strconv.FormatInt(int64(v), 10)).(T), nil
				}
			case reflect.Int:
				if v, ok := value.(int); ok {
					return any(strconv.Itoa(v)).(T), nil
				}
			case reflect.Float64:
				if v, ok := value.(float64); ok {
					return any(strconv.FormatFloat(v, 'f', -1, 64)).(T), nil
				}
			case reflect.Float32:
				if v, ok := value.(float32); ok {
					return any(strconv.FormatFloat(float64(v), 'f', -1, 32)).(T), nil
				}
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					return any(strconv.FormatBool(v)).(T), nil
				}
			case reflect.Slice:
				if sourceType.Elem().Kind() == reflect.Uint8 { // []byte
					if v, ok := value.([]byte); ok {
						return any(string(v)).(T), nil
					}
				}
				// Handle other slices as JSON
				if b, err := json.Marshal(value); err == nil {
					return any(string(b)).(T), nil
				}
			case reflect.Struct:
				if sourceType.String() == "time.Time" {
					if v, ok := value.(time.Time); ok {
						return any(v.Format(time.RFC3339)).(T), nil
					}
				}
				// Handle structs as JSON
				if b, err := json.Marshal(value); err == nil {
					return any(string(b)).(T), nil
				}
			case reflect.Map, reflect.Array:
				// Handle as JSON
				if b, err := json.Marshal(value); err == nil {
					return any(string(b)).(T), nil
				}
			default:
				panic("unhandled default case")
			}

			// Fallback: convert to string using fmt.Sprintf
			return any(fmt.Sprintf("%v", value)).(T), nil
		}, nil

	// ===================
	// UINT64 CONVERSIONS
	// ===================
	case reflect.Uint64:
		return func(value any) (T, error) {
			if value == nil {
				return any(uint64(0)).(T), nil
			}

			switch sourceKind {
			case reflect.Uint64:
				if v, ok := value.(uint64); ok {
					return any(v).(T), nil
				}
			case reflect.Int64:
				if v, ok := value.(int64); ok {
					if v < 0 {
						return any(uint64(0)).(T), fmt.Errorf("negative value %d cannot convert to uint64", v)
					}
					return any(uint64(v)).(T), nil
				}
			case reflect.Int32:
				if v, ok := value.(int32); ok {
					if v < 0 {
						return any(uint64(0)).(T), fmt.Errorf("negative value %d cannot convert to uint64", v)
					}
					return any(uint64(v)).(T), nil
				}
			case reflect.Int:
				if v, ok := value.(int); ok {
					if v < 0 {
						return any(uint64(0)).(T), fmt.Errorf("negative value %d cannot convert to uint64", v)
					}
					return any(uint64(v)).(T), nil
				}
			case reflect.Uint32:
				if v, ok := value.(uint32); ok {
					return any(uint64(v)).(T), nil
				}
			case reflect.String:
				if v, ok := value.(string); ok {
					if v == "" || v == "NULL" || v == "null" {
						return any(uint64(0)).(T), nil
					}
					if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
						return any(parsed).(T), nil
					} else {
						return any(uint64(0)).(T), err
					}
				}
			case reflect.Float64:
				if v, ok := value.(float64); ok {
					if v < 0 || v != float64(uint64(v)) {
						return any(uint64(0)).(T), fmt.Errorf("cannot convert %f to uint64", v)
					}
					return any(uint64(v)).(T), nil
				}
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					if v {
						return any(uint64(1)).(T), nil
					}
					return any(uint64(0)).(T), nil
				}
			default:
				panic("unhandled default case")
			}

			return any(uint64(0)).(T), fmt.Errorf("cannot convert %T to uint64", value)
		}, nil

	// ===================
	// INT64 CONVERSIONS
	// ===================
	case reflect.Int64:
		return func(value any) (T, error) {
			if value == nil {
				return any(int64(0)).(T), nil
			}

			switch sourceKind {
			case reflect.Int64:
				if v, ok := value.(int64); ok {
					return any(v).(T), nil
				}
			case reflect.Uint64:
				if v, ok := value.(uint64); ok {
					if v > 9223372036854775807 { // max int64
						return any(int64(0)).(T), fmt.Errorf("uint64 %d too large for int64", v)
					}
					return any(int64(v)).(T), nil
				}
			case reflect.Int32:
				if v, ok := value.(int32); ok {
					return any(int64(v)).(T), nil
				}
			case reflect.Int:
				if v, ok := value.(int); ok {
					return any(int64(v)).(T), nil
				}
			case reflect.String:
				if v, ok := value.(string); ok {
					if v == "" || v == "NULL" || v == "null" {
						return any(int64(0)).(T), nil
					}
					if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
						return any(parsed).(T), nil
					} else {
						return any(int64(0)).(T), err
					}
				}
			case reflect.Float64:
				if v, ok := value.(float64); ok {
					if v != float64(int64(v)) {
						return any(int64(0)).(T), fmt.Errorf("cannot convert %f to int64: precision loss", v)
					}
					return any(int64(v)).(T), nil
				}
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					if v {
						return any(int64(1)).(T), nil
					}
					return any(int64(0)).(T), nil
				}
			default:
				panic("unhandled default case")
			}

			return any(int64(0)).(T), fmt.Errorf("cannot convert %T to int64", value)
		}, nil

	// ===================
	// FLOAT64 CONVERSIONS
	// ===================
	case reflect.Float64:
		return func(value any) (T, error) {
			if value == nil {
				return any(float64(0)).(T), nil
			}

			switch sourceKind {
			case reflect.Float64:
				if v, ok := value.(float64); ok {
					return any(v).(T), nil
				}
			case reflect.Float32:
				if v, ok := value.(float32); ok {
					return any(float64(v)).(T), nil
				}
			case reflect.Int64:
				if v, ok := value.(int64); ok {
					return any(float64(v)).(T), nil
				}
			case reflect.Uint64:
				if v, ok := value.(uint64); ok {
					return any(float64(v)).(T), nil
				}
			case reflect.Int32:
				if v, ok := value.(int32); ok {
					return any(float64(v)).(T), nil
				}
			case reflect.String:
				if v, ok := value.(string); ok {
					if v == "" || v == "NULL" || v == "null" {
						return any(float64(0)).(T), nil
					}
					if parsed, err := strconv.ParseFloat(v, 64); err == nil {
						return any(parsed).(T), nil
					} else {
						return any(float64(0)).(T), err
					}
				}
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					if v {
						return any(float64(1)).(T), nil
					}
					return any(float64(0)).(T), nil
				}
			default:
				panic("unhandled default case")
			}

			return any(float64(0)).(T), fmt.Errorf("cannot convert %T to float64", value)
		}, nil

	// ===================
	// BOOL CONVERSIONS
	// ===================
	case reflect.Bool:
		return func(value any) (T, error) {
			if value == nil {
				return any(false).(T), nil
			}

			switch sourceKind {
			case reflect.Bool:
				if v, ok := value.(bool); ok {
					return any(v).(T), nil
				}
			case reflect.String:
				if v, ok := value.(string); ok {
					v = strings.ToLower(v)
					switch v {
					case "true", "t", "1", "yes", "y":
						return any(true).(T), nil
					case "false", "f", "0", "no", "n", "", "null":
						return any(false).(T), nil
					default:
						return any(false).(T), fmt.Errorf("cannot parse '%s' as bool", v)
					}
				}
			case reflect.Int64:
				if v, ok := value.(int64); ok {
					return any(v != 0).(T), nil
				}
			case reflect.Uint64:
				if v, ok := value.(uint64); ok {
					return any(v != 0).(T), nil
				}
			case reflect.Int32:
				if v, ok := value.(int32); ok {
					return any(v != 0).(T), nil
				}
			case reflect.Float64:
				if v, ok := value.(float64); ok {
					return any(v != 0).(T), nil
				}
			default:
				panic("unhandled default case")
			}

			return any(false).(T), fmt.Errorf("cannot convert %T to bool", value)
		}, nil

	// ===================
	// TIME CONVERSIONS
	// ===================
	case reflect.Struct:
		if destType.String() == "time.Time" {
			return func(value any) (T, error) {
				if value == nil {
					return any(time.Time{}).(T), nil
				}

				switch sourceKind {
				case reflect.Struct:
					if sourceType.String() == "time.Time" {
						if v, ok := value.(time.Time); ok {
							return any(v).(T), nil
						}
					}
				case reflect.String:
					if v, ok := value.(string); ok {
						if v == "" || v == "NULL" || v == "null" {
							return any(time.Time{}).(T), nil
						}

						// Try common time formats
						formats := []string{
							time.RFC3339,
							time.RFC3339Nano,
							"2006-01-02 15:04:05",
							"2006-01-02T15:04:05",
							"2006-01-02",
							"15:04:05",
						}

						for _, format := range formats {
							if t, err := time.Parse(format, v); err == nil {
								return any(t).(T), nil
							}
						}

						return any(time.Time{}).(T), fmt.Errorf("cannot parse time string: %s", v)
					}
				case reflect.Int64:
					// Unix timestamp
					if v, ok := value.(int64); ok {
						return any(time.Unix(v, 0)).(T), nil
					}
				default:
					panic("unhandled default case")
				}

				return any(time.Time{}).(T), fmt.Errorf("cannot convert %T to time.Time", value)
			}, nil
		}

	// ===================
	// SLICE CONVERSIONS (including vectors)
	// ===================
	case reflect.Slice:
		elemType := destType.Elem()

		// []byte conversions
		if elemType.Kind() == reflect.Uint8 {
			return func(value any) (T, error) {
				if value == nil {
					return any([]byte{}).(T), nil
				}

				switch sourceKind {
				case reflect.String:
					if v, ok := value.(string); ok {
						return any([]byte(v)).(T), nil
					}
				case reflect.Slice:
					if sourceType.Elem().Kind() == reflect.Uint8 {
						if v, ok := value.([]byte); ok {
							return any(v).(T), nil
						}
					}
				default:
					panic("unhandled default case")
				}

				return any([]byte{}).(T), fmt.Errorf("cannot convert %T to []byte", value)
			}, nil
		}

		// []float32 conversions (vectors)
		if elemType.Kind() == reflect.Float32 {
			return func(value any) (T, error) {
				if value == nil {
					return any([]float32{}).(T), nil
				}

				switch sourceKind {
				case reflect.Slice:
					if sourceType.Elem().Kind() == reflect.Float32 {
						if v, ok := value.([]float32); ok {
							return any(v).(T), nil
						}
					}
					if sourceType.Elem().Kind() == reflect.Float64 {
						if v, ok := value.([]float64); ok {
							result := make([]float32, len(v))
							for i, f := range v {
								result[i] = float32(f)
							}
							return any(result).(T), nil
						}
					}
				case reflect.String:
					// Parse JSON array
					if v, ok := value.(string); ok {
						var result []float32
						if err := json.Unmarshal([]byte(v), &result); err == nil {
							return any(result).(T), nil
						}
					}
				default:
					panic("unhandled default case")
				}

				return any([]float32{}).(T), fmt.Errorf("cannot convert %T to []float32", value)
			}, nil
		}

		// []float64 conversions (vectors)
		if elemType.Kind() == reflect.Float64 {
			return func(value any) (T, error) {
				if value == nil {
					return any([]float64{}).(T), nil
				}

				switch sourceKind {
				case reflect.Slice:
					if sourceType.Elem().Kind() == reflect.Float64 {
						if v, ok := value.([]float64); ok {
							return any(v).(T), nil
						}
					}
					if sourceType.Elem().Kind() == reflect.Float32 {
						if v, ok := value.([]float32); ok {
							result := make([]float64, len(v))
							for i, f := range v {
								result[i] = float64(f)
							}
							return any(result).(T), nil
						}
					}
				case reflect.String:
					// Parse JSON array
					if v, ok := value.(string); ok {
						var result []float64
						if err := json.Unmarshal([]byte(v), &result); err == nil {
							return any(result).(T), nil
						}
					}
				default:
					panic("unhandled default case")
				}

				return any([]float64{}).(T), fmt.Errorf("cannot convert %T to []float64", value)
			}, nil
		}

		// []string conversions
		if elemType.Kind() == reflect.String {
			return func(value any) (T, error) {
				if value == nil {
					return any([]string{}).(T), nil
				}

				switch sourceKind {
				case reflect.Slice:
					if sourceType.Elem().Kind() == reflect.String {
						if v, ok := value.([]string); ok {
							return any(v).(T), nil
						}
					}
				case reflect.String:
					// Parse JSON array or comma-separated
					if v, ok := value.(string); ok {
						var result []string
						if err := json.Unmarshal([]byte(v), &result); err == nil {
							return any(result).(T), nil
						}
						// Try comma-separated
						parts := strings.Split(v, ",")
						for i := range parts {
							parts[i] = strings.TrimSpace(parts[i])
						}
						return any(parts).(T), nil
					}
				default:
					panic("unhandled default case")
				}

				return any([]string{}).(T), fmt.Errorf("cannot convert %T to []string", value)
			}, nil
		}

	// ===================
	// MAP CONVERSIONS
	// ===================
	case reflect.Map:
		return func(value any) (T, error) {
			if value == nil {
				var zero T
				return zero, nil
			}

			switch sourceKind {
			case reflect.Map:
				// Direct assignment if same type
				if v, ok := value.(T); ok {
					return v, nil
				}
			case reflect.String:
				// Parse JSON
				if v, ok := value.(string); ok {
					var result T
					if err := json.Unmarshal([]byte(v), &result); err == nil {
						return result, nil
					}
				}
			default:
				panic("unhandled default case")
			}

			var zero T
			return zero, fmt.Errorf("cannot convert %T to map", value)
		}, nil

	// ===================
	// INTERFACE CONVERSIONS
	// ===================
	case reflect.Interface:
		return func(value any) (T, error) {
			if value == nil {
				var zero T
				return zero, nil
			}

			// For interface{}, just return the value
			if destType.String() == "interface {}" {
				return value.(T), nil
			}

			var zero T
			return zero, fmt.Errorf("cannot convert %T to interface", value)
		}, nil
	default:
		panic("unhandled default case")
	}

	// ===================
	// FALLBACK
	// ===================
	return nil, fmt.Errorf("unsupported conversion from %s to %s", sourceType.String(), destType.String())
}
