package schema

import (
	"reflect"
	"sync"
	"unsafe"
)

// RowScanner abstracts database row scanning operations.
// Implemented by sql.Rows, sql.Row, and compatible types.
type RowScanner interface {
	// Scan copies column values into provided destinations.
	// Compatible with database/sql.Rows.Scan() signature.
	Scan(dest ...any) error

	// Columns returns the column names for the result set.
	// Compatible with database/sql.Rows.Columns() signature.
	Columns() ([]string, error)
}

// ScannerFunc defines custom row scanning logic for complex struct types.
// Allows complete override of default field-by-field scanning behavior.
//
// Parameters:
//   - dest: Destination struct to populate
//   - scanner: Database row scanner (sql.Rows, sql.Row, etc.)
//
// Returns: Error if scanning fails
type ScannerFunc func(dest any, scanner RowScanner) error

// ScanConfig provides configuration options for scanning behavior
type ScanConfig struct {
	SkipUnknownColumns bool // Whether to ignore columns not present in struct
}

// DefaultScanConfig provides sensible defaults for most scanning operations
var DefaultScanConfig = ScanConfig{
	SkipUnknownColumns: true,
}

// scannerRegistry maps struct types to their custom scanner functions
// Uses sync.Map for concurrent access during high-frequency scanning operations
var scannerRegistry sync.Map // map[reflect.Type]ScannerFunc

// colBind represents optimized binding between database column and struct field setter.
// Pre-compiled for maximum performance during row scanning operations.
type colBind struct {
	index     int                       // Column index in result set for fast array access
	directSet func(unsafe.Pointer, any) // Pre-compiled field setter function
	fieldType reflect.Type              // Field type for value pointer pool management
}

// RegisterScanner registers a custom scanning function for a specific struct type.
// Use for complex types requiring specialized deserialization, custom validation,
// or performance optimization beyond the default scanning pipeline.
//
// The custom scanner function receives the target struct and a FieldBinder to
// establish column-to-field mappings. This allows fine-grained control over
// which fields are bound and how values are processed.
//
// Parameters:
//   - entity: Zero-value instance of the target struct type
//   - fn: Custom scanning function that handles binding and processing
//
// Example:
//
//	RegisterScanner(User{}, func(target any, binder FieldBinder) error {
//	    user := target.(*User)
//	    // Custom validation or preprocessing
//	    if user.ID < 0 {
//	        return errors.New("invalid user ID")
//	    }
//	    // Establish field bindings
//	    return binder.Bind(user, &user.ID, &user.Name, &user.Email)
//	})
//
// Performance: Registration is typically done once during application startup
func RegisterScanner[T any](entity T, fn func(any, FieldBinder) error) {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	scannerRegistry.Store(t, wrapScanner(fn))
}

// RegisterScannerWithConfig registers a custom scanner with specific configuration options.
// Provides additional control over scanning behavior for specialized use cases.
//
// Parameters:
//   - entity: Zero-value instance of the target struct type
//   - config: Scanning configuration options
//   - fn: Custom scanning function with configuration-aware behavior
//
// Example:
//
//	config := ScanConfig{SkipUnknownColumns: false, CaseSensitive: true}
//	RegisterScannerWithConfig(User{}, config, func(target any, binder FieldBinder) error {
//	    // Configuration-aware scanning logic
//	    return customScanLogic(target, binder)
//	})
func RegisterScannerWithConfig[T any](entity T, config ScanConfig, fn func(any, FieldBinder) error) {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	scannerRegistry.Store(t, wrapScannerWithConfig(fn, config))
}

// =========================================================================
// Scanner Implementation
// =========================================================================

// wrapScanner creates an optimized ScannerFunc from user-provided scanning function.
// Handles the complete scanning pipeline including memory pool management,
// column binding, value scanning, and direct field assignment.
//
// The wrapper provides:
// - Automatic memory pool management for reduced allocations
// - Optimized column-to-field binding with O(1) lookup
// - Type-safe value pointer management
// - Error handling with proper cleanup
// - Direct field assignment using unsafe pointers
//
// Parameters:
//   - fn: User-provided scanning function for field binding
//
// Returns:
//   - ScannerFunc: Optimized scanner with full pipeline implementation
func wrapScanner(fn func(any, FieldBinder) error) ScannerFunc {
	return wrapScannerWithConfig(fn, DefaultScanConfig)
}

// wrapScannerWithConfig creates a configuration-aware scanner wrapper.
// Extends wrapScanner with custom configuration options for specialized scanning behavior.
func wrapScannerWithConfig(fn func(any, FieldBinder) error, config ScanConfig) ScannerFunc {
	return func(target any, row RowScanner) error {
		// Get pooled field binder for memory efficiency
		binder := newBinder(target)
		defer returnBinder(binder)

		// Execute user-defined binding logic
		if err := fn(target, binder); err != nil {
			return err
		}

		// Get column information from database row
		columns, err := row.Columns()
		if err != nil {
			return err
		}

		if len(columns) == 0 {
			return nil // No columns to scan
		}

		// Get struct metadata with LRU caching for performance
		typ := reflect.TypeOf(target)
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		meta, err := Introspect(typ)
		if err != nil {
			return err
		}

		// Get pooled slices using centralized pool management
		colBinds := GetColBinds()
		defer PutColBinds(colBinds)

		dests, cleanup := GetDestinationSlice(len(columns))
		defer cleanup()

		// Build column bindings and scan destinations with type safety
		boundCount := 0
		bindings := binder.Bindings()

		for i, col := range columns {
			// Handle case sensitivity based on configuration
			columnKey := col
			if !schemaContext.caseSensitive {
				columnKey = schemaContext.namingStrategy.ColumnName(col)
			}

			if fm, exists := meta.ColumnMap[columnKey]; exists && fm.DirectSet != nil {
				if _, bound := bindings[col]; bound {
					// Get typed value pointer from centralized pool
					valPtr := GetValuePtr(fm.Type)
					dests[i] = valPtr

					colBinds = append(colBinds, colBind{
						index:     i,
						directSet: fm.DirectSet,
						fieldType: fm.Type,
					})
					boundCount++
					continue
				}
			}

			// Handle unknown columns based on configuration
			if !config.SkipUnknownColumns {
				// Could log warning or return error for unknown columns
			}

			// Use shared dummy destination for unbound columns
			dests[i] = GetGlobalDummy()
		}

		if boundCount == 0 {
			return nil // No bound columns, nothing to scan
		}

		// Perform database row scanning with error handling
		if err := row.Scan(dests...); err != nil {
			// Cleanup allocated value pointers on scan error
			for _, cb := range colBinds {
				PutValuePtr(cb.fieldType, dests[cb.index])
			}
			return err
		}

		// Apply scanned values to struct fields using direct assignment
		structPtr := unsafe.Pointer(reflect.ValueOf(target).Pointer())

		for _, cb := range colBinds {
			valPtr := dests[cb.index]
			// Direct field assignment bypassing reflection
			cb.directSet(structPtr, valPtr)
			// Return value pointer to pool immediately after use
			PutValuePtr(cb.fieldType, valPtr)
		}

		return nil
	}
}

// =========================================================================
// Scanner Lookup
// =========================================================================

// getRegisteredScanner retrieves a custom scanner function for the given type.
// Returns nil if no custom scanner is registered, allowing fallback to default scanning.
//
// This function provides O(1) lookup performance using sync.Map for concurrent access
// during high-frequency scanning operations.
//
// Parameters:
//   - t: reflect.Type to lookup custom scanner for
//
// Returns:
//   - ScannerFunc: Custom scanner if registered, nil otherwise
//
// Example:
//
//	scanner := getRegisteredScanner(reflect.TypeOf(User{}))
//	if scanner != nil {
//	    return scanner(target, row)
//	}
//	// Fall back to default scanning
func getRegisteredScanner(t reflect.Type) ScannerFunc {
	if v, ok := scannerRegistry.Load(t); ok {
		return v.(ScannerFunc)
	}
	return nil
}

// HasRegisteredScanner checks if a custom scanner exists for the given type.
// Useful for conditional logic and performance optimization decisions.
//
// Parameters:
//   - t: reflect.Type to check for custom scanner registration
//
// Returns:
//   - bool: True if custom scanner is registered, false otherwise
//
// Example:
//
//	if HasRegisteredScanner(reflect.TypeOf(User{})) {
//	    log.Debug("Using custom scanner for User type")
//	}
func HasRegisteredScanner(t reflect.Type) bool {
	_, exists := scannerRegistry.Load(t)
	return exists
}

// GetRegisteredScanners returns all currently registered custom scanners.
// Useful for debugging, monitoring, and administrative operations.
//
// Returns:
//   - map[reflect.Type]ScannerFunc: Copy of all registered scanners
//
// Note: Returns a copy to prevent modification of internal registry
func GetRegisteredScanners() map[reflect.Type]ScannerFunc {
	result := make(map[reflect.Type]ScannerFunc)
	scannerRegistry.Range(func(key, value any) bool {
		result[key.(reflect.Type)] = value.(ScannerFunc)
		return true
	})
	return result
}

// ClearRegisteredScanners removes all registered custom scanners.
// Primarily useful for testing and dynamic configuration scenarios.
//
// Example:
//
//	// In test cleanup
//	defer ClearRegisteredScanners()
func ClearRegisteredScanners() {
	scannerRegistry.Range(func(key, value any) bool {
		scannerRegistry.Delete(key)
		return true
	})
}
