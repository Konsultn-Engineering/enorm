package schema

import "reflect"

type FieldScanFunc func(ptr any, val any)

type FieldRegistry interface {
	Bind(entity any, fields ...any) error
	GetBinds() map[string]func(model any, val any)
}

type EntityMeta struct {
	Type         reflect.Type
	Name         string
	Plural       string
	Fields       []*FieldMeta
	FieldMap     map[string]*FieldMeta
	SnakeMap     map[string]*FieldMeta
	ScannerFn    ScannerFunc
	AliasMapping map[string]string
}

type FieldMeta struct {
	GoName     string
	DBName     string
	Type       reflect.Type
	Index      []int
	Tag        reflect.StructTag
	IsExported bool
	SetFunc    func(model any, val any)
	SetFast    func(ptr any, raw any)
}

type ScannerFunc func(any, RowScanner) error

type RowScanner interface {
	Scan(dest ...any) error
	Columns() ([]string, error)
}

type TableNamer interface {
	TableName() string
}
