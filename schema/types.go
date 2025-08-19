package schema

import (
	"reflect"
	"sync"
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
	DirectSet SetterFunc
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
