package ast

type ColumnDef struct {
	Name       string
	Type       *DataType
	NotNull    bool
	Unique     bool
	PrimaryKey bool
	Default    Node
	References *ForeignKeyRef
	Comment    string
}

type DataTypeKind int

const (
	TypeBasic DataTypeKind = iota
	TypeVector
	TypeArray
)

type DataType struct {
	Kind      DataTypeKind
	Name      string // e.g. "VARCHAR", "INT", "VECTOR"
	Dimension int    // only for TypeVector
	Size      int    // for VARCHAR(n)
	Precision int    // for DECIMAL(p, s)
	Scale     int
}

type ForeignKeyRef struct {
	Table    string
	Columns  []string
	OnDelete string
	OnUpdate string
}

type CreateTableStmt struct {
	Table       *Table
	Columns     []*ColumnDef
	IfNotExists bool
}

func (c *CreateTableStmt) Type() NodeType         { return NodeCreateTable }
func (c *CreateTableStmt) Accept(v Visitor) error { return v.VisitCreateTable(c) }
