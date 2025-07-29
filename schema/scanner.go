package schema

import (
	"reflect"
	"sync"
)

var scannerRegistry sync.Map // map[reflect.Type]ScannerFunc

// colBind represents a binding between a column index and its setter function
type colBind struct {
	index int
	bind  func(any, any)
}

// Pools for reducing allocations in hot paths
var (
	colBindsPool = sync.Pool{
		New: func() any {
			return make([]colBind, 0, 8) // Pre-size for common case
		},
	}
	destsPool = sync.Pool{
		New: func() any {
			return make([]any, 0, 8) // Pre-size for common case
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

		// Get pooled slices
		colBinds := colBindsPool.Get().([]colBind)[:0] // Reset length but keep capacity
		defer colBindsPool.Put(colBinds)
		
		for i, col := range columns {
			if bind, ok := fr.binds[col]; ok {
				colBinds = append(colBinds, colBind{index: i, bind: bind})
			}
		}

		typ := reflect.TypeOf(target)
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}

		meta, err := Introspect(typ)
		if err != nil {
			return err
		}

		// Get pooled destination slice
		dests := destsPool.Get().([]any)
		if cap(dests) < len(columns) {
			dests = make([]any, len(columns))
		} else {
			dests = dests[:len(columns)]
		}
		defer destsPool.Put(dests)
		
		for i, col := range columns {
			if _, ok := fr.binds[col]; ok {
				if fm, ok := meta.SnakeMap[col]; ok {
					valPtr := getValuePtr(fm.Type)
					dests[i] = valPtr
				} else {
					var fallback any
					dests[i] = &fallback
				}
			} else {
				var dummy any
				dests[i] = &dummy
			}
		}

		if err := row.Scan(dests...); err != nil {
			return err
		}

		for _, cb := range colBinds {
			// Optimize: avoid reflect.ValueOf by direct pointer dereferencing
			val := dests[cb.index]
			// Since dests[i] is already a pointer to the value from getValuePtr,
			// we need to dereference it to get the actual value
			actualVal := reflect.ValueOf(val).Elem().Interface()
			cb.bind(target, actualVal)

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
