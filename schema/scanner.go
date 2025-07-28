package schema

import (
	"fmt"
	"reflect"
	"strings"
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
		fr := &fieldRegistry{
			entity: target,
			binds:  map[string]func(model any, val any){},
		}
		if err := fn(target, fr); err != nil {
			return err
		}

		columns, err := row.Columns()
		if err != nil {
			return err
		}

		dests := make([]any, len(columns))
		values := make([]any, len(columns))
		for i, col := range columns {
			if _, ok := fr.binds[col]; ok {
				var val any
				values[i] = &val
				dests[i] = &val
			} else {
				var dummy any
				dests[i] = &dummy
			}
		}

		if err := row.Scan(dests...); err != nil {
			return err
		}

		for i, col := range columns {
			if bind, ok := fr.binds[col]; ok {
				bind(target, reflect.Indirect(reflect.ValueOf(dests[i])).Interface())
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

func DebugBindings(meta *EntityMeta, columns []string) string {
	var b strings.Builder
	b.WriteString("Bindings:\n")
	for i, col := range columns {
		field, ok := meta.SnakeMap[col]
		if ok {
			fmt.Fprintf(&b, "  %2d. %-16s → %-16s (%s)\n", i+1, col, field.GoName, field.Type.Name())
		} else {
			fmt.Fprintf(&b, "  %2d. %-16s → [unbound]\n", i+1, col)
		}
	}
	return b.String()
}
