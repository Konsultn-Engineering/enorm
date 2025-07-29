package schema

import (
	"reflect"
	"sync"
	"time"
)

var valuePools sync.Map // map[reflect.Type]*sync.Pool
var byteSlicePool = &sync.Pool{New: func() any { b := make([]byte, 0, 64); return &b }}
var timePool = &sync.Pool{New: func() any { return new(time.Time) }}
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

func getValuePtr(t reflect.Type) any {
	// Special cases first
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return byteSlicePool.Get()
	}
	if t.Kind() == reflect.Struct && t.String() == "time.Time" {
		return timePool.Get()
	}

	// General case
	if pool, ok := kindPools[t.Kind()]; ok {
		return pool.Get()
	}

	// Fallback to reflect-based pooling
	poolIface, ok := valuePools.Load(t)
	if !ok {
		poolIface, _ = valuePools.LoadOrStore(t, &sync.Pool{
			New: func() any {
				return reflect.New(t).Interface()
			},
		})
	}
	return poolIface.(*sync.Pool).Get()
}

func putValuePtr(t reflect.Type, val any) {
	// Special cases
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		byteSlicePool.Put(val)
		return
	}
	if t.Kind() == reflect.Struct && t.String() == "time.Time" {
		timePool.Put(val)
		return
	}

	// General case
	if pool, ok := kindPools[t.Kind()]; ok {
		pool.Put(val)
		return
	}

	// Fallback
	if poolIface, ok := valuePools.Load(t); ok {
		poolIface.(*sync.Pool).Put(val)
	}
}
