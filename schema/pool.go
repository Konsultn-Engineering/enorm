package schema

import (
	"reflect"
	"sync"
)

var valuePools sync.Map // map[reflect.Type]*sync.Pool

func getValuePtr(t reflect.Type) any {
	poolIface, ok := valuePools.Load(t)
	if !ok {
		poolIface, _ = valuePools.LoadOrStore(t, &sync.Pool{
			New: func() any {
				return reflect.New(t).Interface()
			},
		})
	}
	pool := poolIface.(*sync.Pool)
	return pool.Get()
}

func putValuePtr(t reflect.Type, val any) {
	if poolIface, ok := valuePools.Load(t); ok {
		pool := poolIface.(*sync.Pool)
		pool.Put(val)
	}
}
