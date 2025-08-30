package schema

import (
	"database/sql"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// Core type definitions for the schema introspection system.
// Provides high-performance struct metadata and field manipulation capabilities.

// SetterFunc defines the signature for optimized field setter functions.
// Uses unsafe pointers for maximum performance in database row scanning operations.
type SetterFunc func(structPtr unsafe.Pointer, value any)

// FieldScanFunc represents a custom field scanning function for specialized types.
// Deprecated: Use SetterFunc with DirectSet for better performance.
type FieldScanFunc func(ptr any, val any)

// EntityMeta contains complete metadata about a struct type optimized for database operations.
// Includes pre-compiled field setters, lookup maps, and caching for high-performance scanning.
type EntityMeta struct {
	// Type information
	Type reflect.Type // The struct's reflect.Type
	Name string       // Struct name (e.g., "User")

	// Table mapping
	HasCustomTableName bool   // True if struct implements TableNamer interface
	TableName          string // Database table name (custom or pluralized struct name)

	// Field collections and lookup maps for O(1) access
	Fields    []*FieldMeta          // Ordered slice of all mappable fields
	FieldMap  map[string]*FieldMeta // Go field name -> FieldMeta (e.g., "FirstName" -> FieldMeta)
	ColumnMap map[string]*FieldMeta // Database column name -> FieldMeta (e.g., "first_name" -> FieldMeta)
	Columns   []string

	// Additional mappings for flexibility
	AliasMapping map[string]string // Database column -> Go field name (e.g., "first_name" -> "FirstName")

	// Performance optimizations
	preallocatedScanVals []interface{} // Reusable slice for scan operations to reduce allocations
	scanValsMu           sync.Mutex    // Protects preallocatedScanVals for thread safety

	// Custom scanning
	ScannerFn ScannerFunc // Optional custom scanner function for complex types

	pointerFactory     func(unsafe.Pointer) []interface{} // The magic function
	pointerFactoryOnce sync.Once                          // Build it exactly once
	maxColumns         int                                // For slice pre-sizing
}

// GetScanVals returns a properly sized slice for database scanning operations.
// Reuses internal allocation when possible to minimize garbage collection pressure.
// Thread-safe for concurrent access during database operations.
//
// Parameters:
//   - size: Required slice length for the scan operation
//
// Returns: Slice of interface{} ready for sql.Rows.Scan() usage
func (m *EntityMeta) GetScanVals(size int) []interface{} {
	m.scanValsMu.Lock()
	defer m.scanValsMu.Unlock()

	// Resize internal slice if current capacity is insufficient
	if cap(m.preallocatedScanVals) < size {
		m.preallocatedScanVals = make([]interface{}, size)
	} else {
		// Reuse existing allocation, just adjust length
		m.preallocatedScanVals = m.preallocatedScanVals[:size]

		// Clear previous values to prevent memory leaks
		for i := range m.preallocatedScanVals {
			m.preallocatedScanVals[i] = nil
		}
	}

	return m.preallocatedScanVals
}

// ScanAndSet performs high-performance database row scanning into a struct.
// Maps database columns to struct fields and applies optimized type conversions.
//
// Uses unsafe pointer operations and pre-compiled setters for maximum performance.
// Handles type conversions automatically through the converter system.
//
// Parameters:
//   - dest: Pointer to destination struct (must be pointer to struct)
//   - columns: Database column names from sql.Rows.Columns()
//   - scanVals: Scanned values from sql.Rows.Scan()
//
// Returns: Error only if struct pointer is invalid (field mapping errors are ignored)
//
// Performance: ~2-5μs per row for typical structs with 5-10 fields
func (m *EntityMeta) ScanAndSet(dest any, columns []string, scanVals []any) error {
	// Get struct pointer for unsafe operations
	structVal := reflect.ValueOf(dest).Elem()
	structPtr := unsafe.Pointer(structVal.UnsafeAddr())

	// Map database columns to struct fields and apply conversions
	for i, col := range columns {
		fieldMeta := m.ColumnMap[col]
		if fieldMeta == nil {
			// Ignore unmapped columns (allows SELECT * with extra columns)
			continue
		}

		// Use pre-compiled setter for maximum performance
		// Handles all type conversions and null value processing
		fieldMeta.DirectSet(structPtr, scanVals[i])
	}

	return nil
}

// FieldMeta contains complete metadata about a single struct field.
// Includes database mapping information, type conversion data, and optimized setters.
type FieldMeta struct {
	// Field identification
	Name   string // Go field name (e.g., "FirstName")
	DBName string // Database column name (e.g., "first_name", can be any case/format)

	// Type information
	Type   reflect.Type // Go field type (e.g., reflect.TypeOf(""))
	DBType string       // Expected database type (usually same as Type, may differ for custom types)

	// Reflection metadata
	Index      []int   // Field index path for reflect.Value.FieldByIndex()
	IsExported bool    // True if field is exported (always true for processed fields)
	Offset     uintptr // Byte offset within struct for unsafe pointer arithmetic

	// Database mapping configuration
	Tag *ParsedTag // Parsed struct tag with database mapping rules

	// ID generation
	Generator IDGenerator // Optional ID generator for auto-generated primary keys

	// High-performance field setter using unsafe pointers and pre-compiled type conversions
	// Bypasses reflection for maximum speed during database row scanning
	DirectSet    SetterFunc
	PointerMaker func(unsafe.Pointer) interface{} // The optimized pointer creator
}

func (fm *FieldMeta) buildPointerMaker() {
	offset := fm.Offset
	fieldType := fm.Type

	switch fieldType.Kind() {
	// === INTEGER TYPES ===
	case reflect.Bool:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*bool)(unsafe.Add(structPtr, offset))
		}
	case reflect.Int:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*int)(unsafe.Add(structPtr, offset))
		}
	case reflect.Int8:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*int8)(unsafe.Add(structPtr, offset))
		}
	case reflect.Int16:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*int16)(unsafe.Add(structPtr, offset))
		}
	case reflect.Int32:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*int32)(unsafe.Add(structPtr, offset))
		}
	case reflect.Int64:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*int64)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uint:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uint)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uint8:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uint8)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uint16:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uint16)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uint32:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uint32)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uint64:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uint64)(unsafe.Add(structPtr, offset))
		}
	case reflect.Uintptr:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*uintptr)(unsafe.Add(structPtr, offset))
		}

	// === FLOATING POINT ===
	case reflect.Float32:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*float32)(unsafe.Add(structPtr, offset))
		}
	case reflect.Float64:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*float64)(unsafe.Add(structPtr, offset))
		}
	case reflect.Complex64:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*complex64)(unsafe.Add(structPtr, offset))
		}
	case reflect.Complex128:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*complex128)(unsafe.Add(structPtr, offset))
		}

	// === STRING ===
	case reflect.String:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*string)(unsafe.Add(structPtr, offset))
		}

	// === SLICE (mainly []byte for database) ===
	case reflect.Slice:
		if fieldType.Elem().Kind() == reflect.Uint8 { // []byte
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (*[]byte)(unsafe.Add(structPtr, offset))
			}
		} else {
			// Generic slice - use reflection fallback
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				fieldPtr := unsafe.Add(structPtr, offset)
				return reflect.NewAt(fieldType, fieldPtr).Interface()
			}
		}

	// === POINTER TYPES (for nullable fields) ===
	case reflect.Pointer:
		elemType := fieldType.Elem()
		switch elemType.Kind() {
		case reflect.Bool:
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (**bool)(unsafe.Add(structPtr, offset))
			}
		case reflect.Int:
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (**int)(unsafe.Add(structPtr, offset))
			}
		case reflect.Int64:
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (**int64)(unsafe.Add(structPtr, offset))
			}
		case reflect.String:
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (**string)(unsafe.Add(structPtr, offset))
			}
		case reflect.Float64:
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				return (**float64)(unsafe.Add(structPtr, offset))
			}
		// Add other pointer types as needed
		default:
			// Generic pointer - use reflection
			fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
				fieldPtr := unsafe.Add(structPtr, offset)
				return reflect.NewAt(fieldType, fieldPtr).Interface()
			}
		}

	// === STRUCT TYPES (for embedded structs, time.Time, sql.Null*) ===
	case reflect.Struct:
		fm.PointerMaker = fm.buildStructPointerMaker(fieldType, offset)

	// === INTERFACE (any, interface{}) ===
	case reflect.Interface:
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			return (*interface{})(unsafe.Add(structPtr, offset))
		}

	// === FALLBACK for exotic types ===
	default:
		// Arrays, Maps, Channels, Functions - use reflection
		fm.PointerMaker = func(structPtr unsafe.Pointer) interface{} {
			fieldPtr := unsafe.Add(structPtr, offset)
			return reflect.NewAt(fieldType, fieldPtr).Interface()
		}
	}
}

// buildStructPointerMaker handles common struct types for databases
func (fm *FieldMeta) buildStructPointerMaker(fieldType reflect.Type, offset uintptr) func(unsafe.Pointer) interface{} {
	typeName := fieldType.String()

	switch typeName {
	// === TIME TYPES ===
	case "time.Time":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*time.Time)(unsafe.Add(structPtr, offset))
		}

	// === SQL NULL TYPES ===
	case "sql.NullString":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullString)(unsafe.Add(structPtr, offset))
		}
	case "sql.NullInt64":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullInt64)(unsafe.Add(structPtr, offset))
		}
	case "sql.NullInt32":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullInt32)(unsafe.Add(structPtr, offset))
		}
	case "sql.NullFloat64":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullFloat64)(unsafe.Add(structPtr, offset))
		}
	case "sql.NullBool":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullBool)(unsafe.Add(structPtr, offset))
		}
	case "sql.NullTime":
		return func(structPtr unsafe.Pointer) interface{} {
			return (*sql.NullTime)(unsafe.Add(structPtr, offset))
		}

	// === CUSTOM TYPES (add your common ones) ===
	// case "uuid.UUID":
	//     return func(structPtr unsafe.Pointer) interface{} {
	//         return (*uuid.UUID)(unsafe.Add(structPtr, offset))
	//     }

	// === GENERIC STRUCT FALLBACK ===
	default:
		return func(structPtr unsafe.Pointer) interface{} {
			fieldPtr := unsafe.Add(structPtr, offset)
			return reflect.NewAt(fieldType, fieldPtr).Interface()
		}
	}
}

// TableNamer allows structs to specify custom database table names.
// Implement this interface to override default pluralization behavior.
//
// Example:
//
//	type User struct { ... }
//	func (User) TableName() string { return "app_users" }
type TableNamer interface {
	// TableName returns the database table name for this struct type.
	// Should return a consistent value (not dependent on instance state).
	TableName() string
}
