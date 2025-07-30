package schema

import (
	"reflect"
	"sync"
	"time"
)

// valuePools maintains dynamic pools for complex types that don't have dedicated pools.
// Used as fallback when kindPools doesn't contain a specific reflect.Type.
// Key: reflect.Type, Value: *sync.Pool for that type.
var valuePools sync.Map

// byteSlicePool provides reusable byte slices for []byte columns (BLOB, TEXT, etc.).
// Pre-allocated with 64-byte capacity to handle most common database field sizes
// without additional allocations during append operations.
var byteSlicePool = &sync.Pool{
	New: func() any {
		b := make([]byte, 0, 128)
		return &b
	},
}

// timePool provides reusable time.Time instances for temporal database columns.
// Specialized pool since time.Time is extremely common in database schemas
// and has significant allocation overhead when created repeatedly.
var timePool = sync.Pool{New: func() any { return new(time.Time) }}

// 🆕 OPTIMIZATION 1: Pool for reflect.Value slices to avoid allocations in reflection-heavy operations
// reflectValuePool provides reusable []reflect.Value slices for metadata operations.
// Pre-sized to 10 elements to handle typical struct field counts without reallocation.
var reflectValuePool = sync.Pool{
	New: func() interface{} {
		return make([]reflect.Value, 0, 10)
	},
}

// kindPools maps Go's basic types to their dedicated memory pools.
// Provides fast O(1) lookup for the most common database column types,
// avoiding the overhead of sync.Map operations for primitive values.
var kindPools = map[reflect.Kind]*sync.Pool{
	reflect.Int:     {New: func() any { return new(int) }},
	reflect.Int8:    {New: func() any { return new(int8) }},
	reflect.Int16:   {New: func() any { return new(int16) }},
	reflect.Int32:   {New: func() any { return new(int32) }},
	reflect.Int64:   {New: func() any { return new(int64) }},
	reflect.Uint:    {New: func() any { return new(uint) }},
	reflect.Uint8:   {New: func() any { return new(uint8) }},
	reflect.Uint16:  {New: func() any { return new(uint16) }},
	reflect.Uint32:  {New: func() any { return new(uint32) }},
	reflect.Uint64:  {New: func() any { return new(uint64) }},
	reflect.Float32: {New: func() any { return new(float32) }},
	reflect.Float64: {New: func() any { return new(float64) }},
	reflect.Bool:    {New: func() any { return new(bool) }},
	reflect.String:  {New: func() any { return new(string) }},
}

// getValuePtr retrieves a pooled pointer value for the specified type.
// Uses a three-tier lookup strategy: special cases, kind pools, then dynamic pools.
// Returns a pointer to zero-value instance ready for database scanning.
//
// Performance hierarchy:
//  1. Special cases ([]byte, time.Time) - fastest, direct pool access
//  2. Kind pools - fast map lookup for primitives
//  3. Dynamic pools - slower sync.Map for complex types
func getValuePtr(t reflect.Type) any {
	// Tier 1: Special cases with dedicated pools (fastest path)
	switch {
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		ptr := byteSlicePool.Get().(*[]byte)
		*ptr = (*ptr)[:0] // Reset length but keep capacity
		return ptr
	case t.Kind() == reflect.Struct && t == reflect.TypeOf(time.Time{}): // More efficient comparison
		return timePool.Get()
	}

	// Tier 2: Kind-based pools for primitives (fast map lookup)
	if pool, exists := kindPools[t.Kind()]; exists {
		return pool.Get()
	}

	// Tier 3: Dynamic pooling for complex types (slower but necessary)
	poolIface, _ := valuePools.LoadOrStore(t, &sync.Pool{
		New: func() any {
			return reflect.New(t).Interface()
		},
	})
	return poolIface.(*sync.Pool).Get()
}

// putValuePtr returns a used value pointer back to its appropriate pool for reuse.
// Must be called after scanning operations complete to prevent memory leaks
// and maintain pool efficiency. Follows same lookup hierarchy as getValuePtr.
//
// Critical for performance: failure to call this results in pool depletion
// and falls back to regular allocations, negating pooling benefits.
func putValuePtr(t reflect.Type, val any) {
	// Tier 1: Special cases (fastest return path)
	switch {
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		byteSlicePool.Put(val)
		return
	case t.Kind() == reflect.Struct && t == reflect.TypeOf(time.Time{}):
		// Reset time value to zero before returning to pool
		if timePtr, ok := val.(*time.Time); ok {
			*timePtr = time.Time{}
		}
		timePool.Put(val)
		return
	}

	// Tier 2: Kind-based pools
	if pool, exists := kindPools[t.Kind()]; exists {
		pool.Put(val)
		return
	}

	// Tier 3: Dynamic pools
	if poolIface, ok := valuePools.Load(t); ok {
		poolIface.(*sync.Pool).Put(val)
	}
}

// getReflectValues retrieves a pooled []reflect.Value slice for temporary use.
// Returns an empty slice with pre-allocated capacity to avoid growth allocations.
func getReflectValues() []reflect.Value {
	return reflectValuePool.Get().([]reflect.Value)
}

// putReflectValues returns a used []reflect.Value slice back to the pool.
// Clears all reflect.Value entries to prevent memory leaks and resets length to 0.
// MUST be called after reflect.Value operations complete.
func putReflectValues(vals []reflect.Value) {
	// Clear all reflect.Value entries to prevent holding references
	for i := range vals {
		vals[i] = reflect.Value{}
	}
	// Reset length but keep capacity
	vals = vals[:0]
	reflectValuePool.Put(vals)
}
