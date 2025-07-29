package schema

import (
	"reflect"
	"sync"
)

var scannerRegistry sync.Map // map[reflect.Type]ScannerFunc

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
		if err := fn(target, fr); err != nil {
			return err
		}

		columns, err := row.Columns()
		if err != nil {
			return err
		}

		type colBind struct {
			index int
			bind  func(any, any)
		}

		var colBinds []colBind
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

		dests := make([]any, len(columns))
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
			val := reflect.Indirect(reflect.ValueOf(dests[cb.index])).Interface()
			cb.bind(target, val)

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
