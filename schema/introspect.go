package schema

import (
	"fmt"
	"reflect"
	"sync"
)

var entityCache sync.Map // map[reflect.Type]*EntityMeta

func Introspect(t reflect.Type) (*EntityMeta, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s", t.Kind())
	}
	if meta, ok := entityCache.Load(t); ok {
		return meta.(*EntityMeta), nil
	}
	meta, err := buildMeta(t)
	if err != nil {
		return nil, err
	}
	entityCache.Store(t, meta)
	return meta, nil
}
