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

	// Pre-allocate larger map if needed to reduce rehashing
	expectedSize := len(f.binds) + len(fields)
	if expectedSize > len(f.binds)*2 { // Simple growth heuristic
		newBinds := make(map[string]func(entity any, val any), expectedSize)
		for k, v := range f.binds {
			newBinds[k] = v
		}
		f.binds = newBinds
	}

	// Use pointer arithmetic instead of linear search
	basePtr := structVal.UnsafeAddr()

	for _, fieldPtr := range fields {
		ptrAddr := reflect.ValueOf(fieldPtr).Pointer()
		found := false

		// Fast offset-based lookup instead of reflection
		for _, fm := range meta.Fields {
			if fm.IsExported {
				fieldAddr := basePtr + fm.Offset
				if fieldAddr == ptrAddr {
					if fm.DirectSet != nil {
						// Create a wrapper that adapts DirectSet to the expected signature
						directSet := fm.DirectSet
						f.binds[fm.DBName] = func(entity any, val any) {
							entityPtr := reflect.ValueOf(entity).UnsafePointer()
							directSet(entityPtr, val)
						}
						found = true
						break
					}
					return fmt.Errorf("no DirectSet for field %s", fm.Name)
				}
			}
		}

		if !found {
			return fmt.Errorf("bind field not found in struct")
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
