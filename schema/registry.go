package schema

import (
	"fmt"
	"reflect"
	"sync"
)

// registryPool maintains reusable fieldRegistry instances to minimize allocations
// during frequent database scanning operations. Pre-sized with larger capacity
// to handle typical database schema field counts efficiently.
var registryPool = sync.Pool{
	New: func() any {
		return &fieldRegistry{
			binds: make(map[string]func(entity any, val any), 32), // Increased from 16
		}
	},
}

// fieldRegistry manages the binding between database columns and struct fields,
// providing optimized setter functions for direct field assignment during scanning.
// Maintains entity reference and column-to-setter mappings for fast lookups.
type fieldRegistry struct {
	entity any
	binds  map[string]func(entity any, val any)
}

// newRegistry creates a new fieldRegistry instance from the pool, initializing it
// for the specified entity. Reuses existing map capacity while clearing contents
// to avoid allocations in hot path operations.
func newRegistry(entity any) *fieldRegistry {
	fr := registryPool.Get().(*fieldRegistry)
	fr.entity = entity
	// Fast map clear - preserves underlying capacity
	clear(fr.binds) // Go 1.21+ builtin, faster than range+delete
	return fr
}

// returnRegistry cleans up and returns a fieldRegistry instance to the pool
// for reuse. Ensures proper cleanup of entity reference before pooling.
func returnRegistry(fr *fieldRegistry) {
	fr.entity = nil
	registryPool.Put(fr)
}

// Bind establishes fast setter bindings between struct field pointers and their
// corresponding database column names. Uses reflection introspection and unsafe
// pointer operations to create optimized setter functions for maximum performance.
//
// Parameters:
//   - entity: Pointer to the target struct instance
//   - fields: Variadic list of pointers to struct fields to bind
//
// Returns error if binding fails due to type mismatches or missing metadata.
func (f *fieldRegistry) Bind(entity any, fields ...any) error {
	structVal := reflect.ValueOf(entity)
	if structVal.Kind() != reflect.Ptr {
		return fmt.Errorf("bind target must be a pointer to struct")
	}
	structVal = structVal.Elem()
	structType := structVal.Type()

	// Single introspection call with caching
	meta, err := Introspect(structType)
	if err != nil {
		return err
	}

	// Pre-allocate if we know we'll exceed current capacity
	if len(f.binds)+len(fields) > len(f.binds)*2 {
		newBinds := make(map[string]func(entity any, val any), len(f.binds)+len(fields))
		for k, v := range f.binds {
			newBinds[k] = v
		}
		f.binds = newBinds
	}

	// Cache struct field count to avoid repeated calls
	numFields := structVal.NumField()

	// Optimized field matching with early termination
	for _, fieldPtr := range fields {
		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr {
			return fmt.Errorf("bind field must be pointer")
		}

		fieldInterface := fieldPtr
		found := false

		// Linear search with early break - most structs have < 20 fields
		for i := 0; i < numFields; i++ {
			field := structVal.Field(i)
			if field.CanAddr() && field.Addr().Interface() == fieldInterface {
				structField := structType.Field(i)
				fieldName := structField.Name
				dbName := formatName(fieldName)

				// Direct map lookup - already cached from Introspect()
				if fm, exists := meta.FieldMap[fieldName]; exists && fm.SetFast != nil {
					f.binds[dbName] = fm.SetFast
					found = true
					break // Early termination
				}
				return fmt.Errorf("no SetFast for field %s", fieldName)
			}
		}

		if !found {
			return fmt.Errorf("bind field not found in struct: maybe not a field of the supplied structPtr")
		}
	}
	return nil
}

// GetBinds returns the internal column-to-setter mapping for external access.
// Used by scanning operations to retrieve optimized setter functions for
// bound database columns. Returns direct reference to internal map for performance.
func (f *fieldRegistry) GetBinds() map[string]func(model any, val any) {
	return f.binds
}
