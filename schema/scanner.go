package schema

import (
	"reflect"
	"sync"
	"unsafe"
)

var scannerRegistry sync.Map // map[reflect.Type]ScannerFunc

// colBind represents a binding between a column index and its setter function
type colBind struct {
	index     int
	directSet func(unsafe.Pointer, any) // Direct setter for optimization
}

// Pools for reducing allocations in hot paths
var (
	colBindsPool = sync.Pool{
		New: func() any {
			return make([]colBind, 0, 16) // Pre-size for common case
		},
	}
	destsPool = sync.Pool{
		New: func() any {
			return make([]any, 0, 16) // Pre-size for common case
		},
	}
	dummyPool = sync.Pool{
		New: func() any {
			dummy := new(any)
			return dummy
		},
	}
)

func RegisterScanner[T any](model T, fn func(any, FieldRegistry) error) {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	scannerRegistry.Store(t, wrapScanner(fn))
}

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

		// Get pooled slices
		colBinds := colBindsPool.Get().([]colBind)
		if cap(colBinds) < len(columns) {
			colBinds = make([]colBind, 0, len(columns))
		} else {
			colBinds = colBinds[:0] // Reset length but keep capacity
		}
		defer colBindsPool.Put(colBinds)

		// Build column bindings directly from metadata to avoid map lookups
		for i, col := range columns {
			if fm, ok := meta.SnakeMap[col]; ok && fm.DirectSet != nil {
				// Verify this column was bound in the registry
				if _, bound := fr.binds[col]; bound {
					colBinds = append(colBinds, colBind{
						index:     i,
						directSet: fm.DirectSet,
					})
				}
			}
		}

		// Get pooled destination slice
		dests := destsPool.Get().([]any)
		if cap(dests) < len(columns) {
			dests = make([]any, len(columns))
		} else {
			dests = dests[:len(columns)]
		}
		defer destsPool.Put(dests)

		// Track pooled dummy values for cleanup
		var pooledDummies []any
		defer func() {
			for _, dummy := range pooledDummies {
				dummyPool.Put(dummy)
			}
		}()

		for i, col := range columns {
			if fm, ok := meta.SnakeMap[col]; ok && fm.DirectSet != nil {
				if _, bound := fr.binds[col]; bound {
					valPtr := getValuePtr(fm.Type)
					dests[i] = valPtr
					colBinds = append(colBinds, colBind{
						index:     i,
						directSet: fm.DirectSet,
					})
					continue
				}
			}

			dummy := dummyPool.Get()
			dests[i] = dummy
			pooledDummies = append(pooledDummies, dummy)
		}

		if err := row.Scan(dests...); err != nil {
			return err
		}

		// Get unsafe pointer to struct for direct field access
		structPtr := unsafe.Pointer(reflect.ValueOf(target).Pointer())

		for _, cb := range colBinds {
			// Get the value pointer and use direct setter
			valPtr := dests[cb.index]
			cb.directSet(structPtr, valPtr)

			// Return to pool
			if fm, ok := meta.SnakeMap[columns[cb.index]]; ok {
				putValuePtr(fm.Type, dests[cb.index])
			}
		}

		return nil
	}
}

func getRegisteredScanner(t reflect.Type) ScannerFunc {
	if v, ok := scannerRegistry.Load(t); ok {
		return v.(ScannerFunc)
	}
	return nil
}
