package schema

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"reflect"
	"sync"
	"sync/atomic"
)

var (
	entityCache  *lru.Cache[reflect.Type, *EntityMeta]
	typeToIndex  sync.Map // map[reflect.Type]uint32, safe concurrent map
	indexCounter uint32   // atomic counter
)

func New(size int, onEvict func(key reflect.Type, value *EntityMeta)) {
	var err error
	entityCache, err = lru.New[reflect.Type, *EntityMeta](size)
	if err != nil {
		panic(fmt.Sprintf("failed to create entityCache: %v", err))
	}
}

func Introspect(t reflect.Type) (*EntityMeta, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: %s", t.Kind())
	}

	if meta, ok := entityCache.Get(t); ok {
		return meta, nil
	}

	meta, err := buildMeta(t)
	if err != nil {
		return nil, err
	}

	// Assign static index
	if val, ok := typeToIndex.Load(t); ok {
		meta.Index = val.(uint32)
	} else {
		newIndex := atomic.AddUint32(&indexCounter, 1)
		meta.Index = newIndex
		typeToIndex.Store(t, newIndex)
	}

	entityCache.Add(t, meta)
	return meta, nil
}
