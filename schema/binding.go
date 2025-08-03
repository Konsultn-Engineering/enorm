package schema

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"
)

// FieldBinder provides a pluggable interface for custom field binding logic.
// Allows applications to override default field mapping behavior for specific types.
type FieldBinder interface {
	// Bind associates entity fields with custom binding functions.
	// Returns error if binding configuration is invalid.
	Bind(entity any, fields ...any) error

	// Bindings returns the current field binding configuration.
	// Map keys are field names, values are binding functions.
	Bindings() map[string]func(entity any, val any)
}

// binderPool maintains reusable fieldBinder instances to minimize allocations
// during high-frequency database scanning operations. Pre-sized for typical
// database schema field counts to reduce map rehashing overhead.
var binderPool = sync.Pool{
	New: func() any {
		return &fieldBinder{
			bindings: make(map[string]func(entity any, val any), 32), // Sized for typical table schemas
		}
	},
}

// fieldBinder manages optimized bindings between database columns and struct fields.
// Provides high-performance setter functions for direct field assignment during row scanning.
//
// The binder introspects struct layout once and creates fast setter functions that bypass
// reflection during actual database scanning operations, achieving near-native performance.
//
// Fields:
//   - entity: Target struct instance for field binding
//   - bindings: Column name to optimized setter function mapping
//   - ctx: Schema context for introspection and configuration
type fieldBinder struct {
	entity   any                                  // Target struct instance
	bindings map[string]func(entity any, val any) // Column name -> optimized setter function
	ctx      *Context                             // Schema context for introspection
}

// newBinder creates a fieldBinder instance from the pool for the specified entity.
// Reuses existing map capacity while clearing contents to avoid allocations.
//
// This function is optimized for high-frequency usage in database scanning loops,
// leveraging object pooling to minimize garbage collection pressure.
//
// Parameters:
//   - entity: Target struct instance for field binding (must be struct pointer)
//   - ctx: Schema context for introspection and configuration
//
// Returns:
//   - *fieldBinder: Ready-to-use binder with cleared bindings from pool
//
// Example:
//
//	var user User
//	binder := newBinder(&user, schemaCtx)
//	defer returnBinder(binder)
func newBinder(entity any, ctx *Context) *fieldBinder {
	fb := binderPool.Get().(*fieldBinder)
	fb.entity = entity
	fb.ctx = ctx

	// Fast map clearing - preserves underlying capacity for reuse
	clear(fb.bindings) // Go 1.21+ builtin - faster than range+delete loop

	return fb
}

// returnBinder cleans up and returns a fieldBinder to the pool for reuse.
// Ensures proper cleanup to prevent memory leaks and reference retention.
//
// This function should always be called when finished with a binder to maintain
// optimal memory usage and pool efficiency.
//
// Parameters:
//   - fb: fieldBinder instance to return to pool (will be cleaned and reset)
//
// Example:
//
//	binder := newBinder(&user, ctx)
//	defer returnBinder(binder)
func returnBinder(fb *fieldBinder) {
	fb.entity = nil // Prevent memory leak from retained references
	fb.ctx = nil    // Clear context reference
	binderPool.Put(fb)
}

// Bind establishes optimized setter bindings between struct field pointers and
// their corresponding database column names. Uses unsafe pointer arithmetic
// and pre-compiled setters for maximum scanning performance.
//
// This method introspects the struct once and creates fast setter functions
// that bypass reflection during actual database row scanning operations.
// The binding process uses pointer arithmetic to match field addresses with
// struct field offsets, ensuring type safety while maintaining performance.
//
// Parameters:
//   - entity: Pointer to target struct instance (must be struct pointer)
//   - fields: Variadic list of pointers to struct fields to bind
//
// Returns:
//   - error: Binding error if validation fails or field matching unsuccessful
//
// Errors:
//   - Non-pointer entity or non-struct target
//   - Nil field pointers in fields list
//   - Field pointers not belonging to target struct
//   - Missing DirectSet functions (internal error)
//
// Example:
//
//	var user User
//	binder := newBinder(&user, ctx)
//	err := binder.Bind(&user, &user.ID, &user.Name, &user.Email)
//	if err != nil {
//	    return fmt.Errorf("binding failed: %w", err)
//	}
//
// Performance: O(n*m) where n=fields, m=struct fields, executed once per query
func (fb *fieldBinder) Bind(entity any, fields ...any) error {
	// Validate entity is a struct pointer
	structVal := reflect.ValueOf(entity)
	if structVal.Kind() != reflect.Ptr {
		return fmt.Errorf("bind target must be a pointer to struct, got %T", entity)
	}

	structVal = structVal.Elem()
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("bind target must be pointer to struct, got pointer to %s", structVal.Kind())
	}

	structType := structVal.Type()

	// Single introspection call with LRU caching for metadata using context
	meta, err := fb.ctx.Introspect(structType)
	if err != nil {
		return fmt.Errorf("failed to introspect struct type %s: %w", structType.Name(), err)
	}

	// Optimize map capacity if many bindings are expected
	expectedSize := len(fb.bindings) + len(fields)
	if expectedSize > len(fb.bindings)*2 {
		// Grow map capacity proactively to reduce rehashing
		newBindings := make(map[string]func(entity any, val any), expectedSize*2)
		for k, v := range fb.bindings {
			newBindings[k] = v
		}
		fb.bindings = newBindings
	}

	// Use base pointer for efficient field address calculation
	basePtr := structVal.UnsafeAddr()

	// Bind each field pointer to its corresponding database column
	for i, fieldPtr := range fields {
		if fieldPtr == nil {
			return fmt.Errorf("field pointer %d is nil", i)
		}

		ptrVal := reflect.ValueOf(fieldPtr)
		if ptrVal.Kind() != reflect.Ptr {
			return fmt.Errorf("field %d must be a pointer, got %T", i, fieldPtr)
		}

		ptrAddr := ptrVal.Pointer()
		found := false

		// Efficient field matching using pointer arithmetic instead of reflection
		for _, fm := range meta.Fields {
			if !fm.IsExported {
				continue // Skip unexported fields
			}

			// Calculate expected field address using unsafe pointer arithmetic
			fieldAddr := basePtr + fm.Offset

			if fieldAddr == ptrAddr {
				// Validate that field has a compiled setter
				if fm.DirectSet == nil {
					return fmt.Errorf("field %s lacks DirectSet function - internal error", fm.Name)
				}

				// Create optimized setter wrapper for this specific field
				directSet := fm.DirectSet // Capture for closure
				fb.bindings[fm.DBName] = func(entity any, val any) {
					// Safe conversion from interface{} to unsafe.Pointer
					entityVal := reflect.ValueOf(entity)
					if entityVal.Kind() != reflect.Ptr {
						panic(fmt.Sprintf("entity must be a pointer, got %T", entity))
					}

					// Get the unsafe pointer directly from the reflect.Value
					entityPtr := unsafe.Pointer(entityVal.Pointer())
					directSet(entityPtr, val)
				}

				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("field pointer %d does not belong to struct %s", i, structType.Name())
		}
	}

	return nil
}

// Bindings returns the internal column-to-setter mapping for external scanning operations.
// Used by row scanning logic to retrieve optimized setter functions for bound database columns.
//
// This method provides direct access to the internal binding map for maximum performance
// during database row scanning. The returned map allows O(1) lookup of setter functions
// by database column name.
//
// Returns:
//   - map[string]func(entity any, val any): Column name to setter function mapping
//
// Note:
//
//	Returns the actual internal map (not a copy) for maximum performance.
//	Callers should not modify the returned map as it may be reused from the pool.
//
// Example:
//
//	bindings := binder.Bindings()
//	if setter, exists := bindings["user_id"]; exists {
//	    setter(&user, userIDValue)
//	}
func (fb *fieldBinder) Bindings() map[string]func(entity any, val any) {
	return fb.bindings
}

// BindingCount returns the number of currently bound database columns.
// Useful for debugging, performance monitoring, and validation of binding completeness.
//
// This method provides insight into binding effectiveness and can help identify
// potential performance issues or incomplete column binding scenarios.
//
// Returns:
//   - int: Number of bound columns with associated setter functions
//
// Example:
//
//	count := binder.BindingCount()
//	if count == 0 {
//	    log.Warn("No column bindings established")
//	}
//	log.Infof("Bound %d database columns to struct fields", count)
func (fb *fieldBinder) BindingCount() int {
	return len(fb.bindings)
}

// HasBinding checks if a specific database column has been bound to a struct field.
// Useful for conditional binding logic, validation, and dynamic query building.
//
// This method provides O(1) lookup to determine if a column binding exists,
// enabling efficient conditional processing during database operations.
//
// Parameters:
//   - columnName: Database column name to check for existing binding
//
// Returns:
//   - bool: True if column has an associated setter function, false otherwise
//
// Example:
//
//	if binder.HasBinding("created_at") {
//	    log.Debug("Timestamp binding available")
//	} else {
//	    log.Warn("Missing timestamp binding - using default value")
//	}
func (fb *fieldBinder) HasBinding(columnName string) bool {
	_, exists := fb.bindings[columnName]
	return exists
}
