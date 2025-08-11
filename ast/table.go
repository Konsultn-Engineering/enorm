package ast

import "github.com/Konsultn-Engineering/enorm/utils"

type Table struct {
	Schema string
	Name   string
	Alias  string
}

func NewTable(schema, name, alias string) *Table {
	t := tablePool.Get().(*Table)
	t.Schema = schema
	t.Name = name
	t.Alias = alias
	return t
}

func (t *Table) Type() NodeType         { return NodeTable }
func (t *Table) Accept(v Visitor) error { return v.VisitTable(t) }
func (t *Table) Fingerprint() uint64 {
	s := t.Schema + "." + t.Name + "." + t.Alias
	return utils.FingerprintString(s)
}

func (t *Table) Release() {
	tablePool.Put(t)
}
