package schema

import (
	"reflect"
	"sync"
	"unsafe"
)

// scannerRegistry is a thread-safe registry mapping struct types to their scanner functions.
// Used to store and retrieve custom scanning logic for specific types.
var scannerRegistry sync.Map // map[reflect.Type]ScannerFunc

// colBind represents a binding between a database column index and its corresponding
// struct field setter function. Optimized for direct memory access during scanning.
type colBind struct {
	index     int
	directSet func(unsafe.Pointer, any)
	fieldType reflect.Type // Cache for value pooling
}

// Pools for reducing allocations in hot paths during database scanning operations.
// These pools maintain reusable slices to minimize garbage collection pressure.
var (
	colBindsPool = sync.Pool{
		New: func() any {
			return make([]colBind, 0, 32) // Increased default size
		},
	}
	destsPool = sync.Pool{
		New: func() any {
			return make([]any, 0, 32) // Increased default size
		},
	}
	// Pre-allocated dummy for unbound columns
	globalDummy = new(any)
)

// RegisterScanner registers a custom scanning function for a specific struct type.
// The scanner function will be called during row scanning to handle field binding.
// Type parameter T represents the struct type being registered.
func RegisterScanner[T any](entity T, fn func(any, FieldRegistry) error) {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	scannerRegistry.Store(t, wrapScanner(fn))
}

// wrapScanner creates an optimized ScannerFunc from a user-provided scanning function.
// Handles the complete scanning pipeline including column binding, memory management,
// and direct field assignment using unsafe pointers for maximum performance.
func wrapScanner(fn func(any, FieldRegistry) error) ScannerFunc {
	return func(target any, row RowScanner) error {
		fr := newRegistry(target)
		defer returnRegistry(fr)

		if err := fn(target, fr); err != nil {
			return err
		}

		columns, err := row.Columns()
		if err != nil {
			return err
		}

		typ := reflect.TypeOf(target)
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		meta, err := Introspect(typ)
		if err != nil {
			return err
		}

		// Get pooled slices with better reuse
		colBinds := colBindsPool.Get().([]colBind)
		colBinds = colBinds[:0] // Reset but keep capacity
		defer colBindsPool.Put(colBinds)

		dests := destsPool.Get().([]any)
		if cap(dests) < len(columns) {
			dests = make([]any, len(columns))
		} else {
			dests = dests[:len(columns)]
		}
		defer destsPool.Put(dests)

		// Single pass: build bindings and destinations simultaneously
		boundCount := 0
		for i, col := range columns {
			if fm, ok := meta.ColumnMap[col]; ok && fm.DirectSet != nil {
				if _, bound := fr.binds[col]; bound {
					// Create value pointer for this field type
					valPtr := getValuePtr(fm.Type)
					dests[i] = valPtr

					colBinds = append(colBinds, colBind{
						index:     i,
						directSet: fm.DirectSet,
						fieldType: fm.Type, // Cache for cleanup
					})
					boundCount++
					continue
				}
			}
			// Use global dummy for unbound columns (no allocation)
			dests[i] = globalDummy
		}

		// Early exit if no bound columns
		if boundCount == 0 {
			return nil
		}

		if err := row.Scan(dests...); err != nil {
			// Cleanup value pointers on error
			for _, cb := range colBinds {
				putValuePtr(cb.fieldType, dests[cb.index])
			}
			return err
		}

		// Get unsafe pointer once
		structPtr := unsafe.Pointer(reflect.ValueOf(target).Pointer())

		// Single loop for setting and cleanup
		for _, cb := range colBinds {
			valPtr := dests[cb.index]
			cb.directSet(structPtr, valPtr)
			putValuePtr(cb.fieldType, valPtr) // Immediate cleanup
		}

		return nil
	}
}

// getRegisteredScanner retrieves a previously registered scanner function for the given type.
// Returns nil if no scanner is registered for the specified type.
func getRegisteredScanner(t reflect.Type) ScannerFunc {
	if v, ok := scannerRegistry.Load(t); ok {
		return v.(ScannerFunc)
	}
	return nil
}
