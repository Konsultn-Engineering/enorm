package schema

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

var setterCreators = sync.Map{}

func registerSetterCreator[T any]() {
	var zero T
	zeroType := reflect.TypeOf(zero)

	// Pre-cache common converter types at registration time
	commonConverters := make(map[reflect.Type]func(any) (T, error), 8)

	setterCreators.Store(zeroType, func(offset uintptr) func(unsafe.Pointer, any) {
		return func(structPtr unsafe.Pointer, value any) {
			fieldPtr := (*T)(unsafe.Add(structPtr, offset))

			if value == nil {
				*fieldPtr = zero
				return
			}

			actualValue := value
			var actualValueType reflect.Type

			if val := reflect.ValueOf(value); val.Kind() == reflect.Ptr && !val.IsNil() {
				actualValue = val.Elem().Interface()
				actualValueType = val.Type().Elem() // Use val.Type() instead of new TypeOf call
			} else {
				actualValueType = reflect.TypeOf(value)
			}

			// Check cached converters first
			if cachedConverter, exists := commonConverters[actualValueType]; exists {
				*fieldPtr, _ = cachedConverter(actualValue)
				return
			}

			// Fallback to GetConverter and cache the result
			converter, err := GetConverter(zero, actualValueType)
			if err != nil {
				panic(err)
			}

			// Cache this converter for future use
			if len(commonConverters) < 16 { // Limit cache size
				commonConverters[actualValueType] = func(v any) (T, error) {
					result, err := converter(v)
					return result, err
				}
			}

			*fieldPtr, _ = converter(actualValue)
		}
	})
}
func init() {
	registerSetterCreator[int64]()
	registerSetterCreator[uint64]()
	registerSetterCreator[string]()
	registerSetterCreator[*string]()
	registerSetterCreator[bool]()
	registerSetterCreator[float64]()
	registerSetterCreator[time.Time]()
	registerSetterCreator[[]byte]()
	registerSetterCreator[json.RawMessage]()
	registerSetterCreator[[]float32]() // Vectors
	registerSetterCreator[sql.NullString]()
	registerSetterCreator[sql.NullTime]()
}

func createDirectSetterForType(fieldType reflect.Type, offset uintptr) func(unsafe.Pointer, any) {
	if creator, ok := setterCreators.Load(fieldType); ok {
		return creator.(func(uintptr) func(unsafe.Pointer, any))(offset)
	}

	// Fallback for unregistered types - uses reflection
	return func(structPtr unsafe.Pointer, value any) {
		targetValue := reflect.NewAt(fieldType, unsafe.Add(structPtr, offset)).Elem()
		if value == nil {
			targetValue.Set(reflect.Zero(fieldType))
			return
		}

		actualValue := value
		if val := reflect.ValueOf(value); val.Kind() == reflect.Ptr && !val.IsNil() {
			actualValue = val.Elem().Interface()
			val = reflect.ValueOf(actualValue)
			if val.Type().ConvertibleTo(fieldType) {
				targetValue.Set(val.Convert(fieldType))
				return
			}
		}

		converter, _ := GetConverter(reflect.New(fieldType).Elem().Interface(), reflect.TypeOf(actualValue))
		converted, _ := converter(actualValue)
		targetValue.Set(reflect.ValueOf(converted))
	}
}

// buildMeta constructs comprehensive metadata for a struct type, including
// all field information, setter functions, and lookup maps needed for
// high-performance database operations.
//
// This function performs expensive reflection operations once and caches
// the results to avoid repeated computation during database scanning.
//
// Parameters:
//   - t: reflect.Type of the struct to analyze (pointer types are dereferenced)
//
// Returns: Complete EntityMeta with all pre-compiled optimizations
func buildMeta(t reflect.Type) (*EntityMeta, error) {
	// Normalize pointer types to their element type
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Validate that we're working with a struct
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s (expected struct)", t.Kind())
	}

	// Initialize tag parser for database field mapping
	parser := NewTagParser()

	// Pre-allocate maps with estimated capacity to reduce rehashing
	numFields := t.NumField()

	exportedCount := 0
	for i := 0; i < numFields; i++ {
		if t.Field(i).IsExported() && !t.Field(i).Anonymous {
			exportedCount++
		}
	}

	meta := &EntityMeta{
		Type:         t,
		Name:         t.Name(),
		Fields:       make([]*FieldMeta, 0, exportedCount),
		FieldMap:     make(map[string]*FieldMeta, exportedCount),
		ColumnMap:    make(map[string]*FieldMeta, exportedCount),
		AliasMapping: make(map[string]string, exportedCount),
	}

	// Check for custom table naming interface
	var customTableName string
	var hasCustomName bool
	if tn, ok := reflect.New(t).Interface().(TableNamer); ok {
		customTableName = tn.TableName()
		hasCustomName = true
	}

	meta.HasCustomTableName = hasCustomName
	if hasCustomName {
		meta.TableName = customTableName
	} else {
		meta.TableName = pluralize(t.Name())
	}

	// Get pooled reflect.Values for field processing
	reflectVals := getReflectValues()
	defer putReflectValues(reflectVals)

	// Process each struct field to build comprehensive metadata
	for i := 0; i < numFields; i++ {
		f := t.Field(i)

		// Skip unexported fields and anonymous embedded fields
		// Anonymous fields could be supported in future for composition
		if !f.IsExported() || f.Anonymous {
			continue
		}

		// Parse struct tags for database mapping configuration
		parsedTag, err := parser.ParseTag(f.Name, f.Tag)
		if err != nil {
			return nil, fmt.Errorf("error parsing tag for field %s: %w", f.Name, err)
		}

		// Honor skip directives from struct tags (e.g., `db:"-"`)
		if parsedTag.IsSkipped() {
			continue
		}

		// Build complete field metadata with all optimization functions
		fm := &FieldMeta{
			Name:       f.Name,
			DBName:     parsedTag.ColumnName,
			Type:       f.Type,
			DBType:     parsedTag.Type,
			Index:      f.Index,
			Tag:        parsedTag,
			IsExported: true,
			Offset:     f.Offset, // For unsafe pointer operations
			Generator:  parsedTag.GetGenerator(),
		}

		fm.DirectSet = createDirectSetterForType(f.Type, f.Offset)

		// Build all lookup structures for O(1) field access during scanning
		meta.Fields = append(meta.Fields, fm)
		meta.FieldMap[f.Name] = fm // Go field name -> metadata
		if parsedTag.ColumnName == "" {
			parsedTag.ColumnName = formatName(f.Name)
		}
		meta.ColumnMap[parsedTag.ColumnName] = fm        // DB column name -> metadata
		meta.AliasMapping[parsedTag.ColumnName] = f.Name // DB column -> Go field name
	}

	// Attach custom scanner function if registered for this type
	if fn := getRegisteredScanner(t); fn != nil {
		meta.ScannerFn = fn
	}

	return meta, nil
}
