package builder

import (
	"fmt"
	"strings"
)

type SelectBuilder struct {
	table   string
	columns []string
	where   string
	args    []any
	joins   []string
	orderBy string
	limit   *int
	offset  *int
}

func NewSelect(table string, columns []string) *SelectBuilder {
	return &SelectBuilder{
		table:   table,
		columns: columns,
	}
}

func (b *SelectBuilder) Where(cond string, args ...any) *SelectBuilder {
	b.where = cond
	b.args = args
	return b
}

func (b *SelectBuilder) Join(join string) *SelectBuilder {
	b.joins = append(b.joins, join)
	return b
}

func (b *SelectBuilder) Order(order string) *SelectBuilder {
	b.orderBy = order
	return b
}

func (b *SelectBuilder) Limit(n int) *SelectBuilder {
	b.limit = &n
	return b
}

func (b *SelectBuilder) Offset(n int) *SelectBuilder {
	b.offset = &n
	return b
}

func (b *SelectBuilder) Build() (string, []any) {
	var sb strings.Builder

	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(b.columns, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(b.table)

	for _, j := range b.joins {
		sb.WriteString(" ")
		sb.WriteString(j)
	}

	if b.where != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(b.where)
	}

	if b.orderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(b.orderBy)
	}

	if b.limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *b.limit))
	}

	if b.offset != nil {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", *b.offset))
	}

	return sb.String(), b.args
}
