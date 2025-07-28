package cache

import (
	"reflect"
	"sync"
)

type ScannerPlan struct {
	Type   reflect.Type
	Index  []int
	Offset uintptr
}

type ScannerCache interface {
	GetOrSet(typeID string, plan *ScannerPlan) *ScannerPlan
}

type memScannerCache struct {
	mu   sync.RWMutex
	data map[string]*ScannerPlan
}

func NewScannerCache() ScannerCache {
	return &memScannerCache{
		data: make(map[string]*ScannerPlan),
	}
}

func (c *memScannerCache) GetOrSet(typeID string, plan *ScannerPlan) *ScannerPlan {
	c.mu.RLock()
	if existing, ok := c.data[typeID]; ok {
		c.mu.RUnlock()
		return existing
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[typeID] = plan
	return plan
}
