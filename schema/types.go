package schema

import (
	"reflect"
	"unsafe"
)

type FieldScanFunc func(ptr any, val any)

type FieldRegistry interface {
	Bind(entity any, fields ...any) error
	GetBinds() map[string]func(model any, val any)
}

type EntityMeta struct {
	Type                 reflect.Type
	Name                 string
	HasCustomTableName   bool
	TableName            string
	Fields               []*FieldMeta
	FieldMap             map[string]*FieldMeta  // Go field name -> FieldMeta
	ColumnMap            map[string]*FieldMeta  // Database column name -> FieldMeta (renamed from SnakeMap)
	AddressMap           map[uintptr]*FieldMeta // NEW: field address -> FieldMeta for O(1) lookup
	ScannerFn            ScannerFunc
	AliasMapping         map[string]string
	preallocatedScanVals []interface{}
}

// getScanVals returns the pre-allocated slice, resizing if necessary
func (m *EntityMeta) getScanVals(size int) []interface{} {
	if cap(m.preallocatedScanVals) < size {
		m.preallocatedScanVals = make([]interface{}, size)
	} else {
		m.preallocatedScanVals = m.preallocatedScanVals[:size]
	}
	return m.preallocatedScanVals
}

// ScanAndSet handles scanning from SQL rows and setting fields using DirectSet
func (m *EntityMeta) ScanAndSet(dest any, columns []string, scanVals []any) error {
	structVal := reflect.ValueOf(dest).Elem()
	structPtr := unsafe.Pointer(structVal.UnsafeAddr())

	// Use pre-allocated slice instead of external scanVals to reduce allocations
	internalScanVals := m.getScanVals(len(columns))
	copy(internalScanVals, scanVals)

	for i, col := range columns {
		fieldMeta := m.ColumnMap[col] // Updated reference
		if fieldMeta == nil {
			continue // ignore unmapped fields
		}

		// DirectSet now handles all type conversions dynamically
		fieldMeta.DirectSet(structPtr, internalScanVals[i])
	}

	return nil
}

type FieldMeta struct {
	Name       string
	DBName     string // This could be snake_case, camelCase, PascalCase, etc.
	Type       reflect.Type
	DBType     reflect.Type
	Index      []int
	Tag        *ParsedTag
	IsExported bool
	// Optimization: store the offset for direct field access
	Offset    uintptr
	Generator IDGenerator
	// Direct setter using unsafe pointers for maximum performance
	DirectSet func(structPtr unsafe.Pointer, val any)
}

type ScannerFunc func(any, RowScanner) error

type RowScanner interface {
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type TableNamer interface {
	TableName() string
}
