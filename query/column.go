package query

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"strings"
)

// Col is the fluent API entry point
func Col(name string) *ColumnBuilder {
	return &ColumnBuilder{
		name: name,
	}
}

// ColumnBuilder provides fluent API for complex column specs
type ColumnBuilder struct {
	table string
	name  string
	alias string
}

func (cb *ColumnBuilder) From(table string) *ColumnBuilder {
	cb.table = table
	return cb
}

func (cb *ColumnBuilder) As(alias string) *ColumnBuilder {
	cb.alias = alias
	return cb
}

func (cb *ColumnBuilder) Build() *ast.Column {
	return ast.NewColumn(cb.table, cb.name, cb.alias)
}

// parseColumnString efficiently parses "table.column AS alias" formats
// Returns table, name, alias (any can be empty)
func parseColumnString(spec string) (table, name, alias string) {
	// Handle AS clause first
	if asIdx := strings.Index(strings.ToUpper(spec), " AS "); asIdx > 0 {
		alias = strings.TrimSpace(spec[asIdx+4:])
		spec = strings.TrimSpace(spec[:asIdx])
	}

	// Handle table.column
	if dotIdx := strings.Index(spec, "."); dotIdx > 0 {
		table = spec[:dotIdx]
		name = spec[dotIdx+1:]
	} else {
		name = spec
	}

	return
}
