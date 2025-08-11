package ast

import "github.com/Konsultn-Engineering/enorm/utils"

type Column struct {
	Table string
	Name  string
	Alias string
}

func NewColumn(table, name, alias string) *Column {
	c := columnPool.Get().(*Column)
	c.Table = table
	c.Name = name
	c.Alias = alias
	return c
}

func (c *Column) Type() NodeType { return NodeColumn }

func (c *Column) Accept(v Visitor) error { return v.VisitColumn(c) }

func (c *Column) Fingerprint() uint64 {
	s := c.Table + "." + c.Name
	return utils.FingerprintString(s)
}

func (c *Column) Release() {
	columnPool.Put(c)
}
