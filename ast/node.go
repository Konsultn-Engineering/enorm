package ast

type NodeType int

const (
	NodeSelect NodeType = iota
	NodeInsert
	NodeUpdate
	NodeDelete
	NodeCreateTable
	NodeColumn
	NodeTable
	NodeValue
	NodeArray
	NodeFunction
	NodeGroupedExpr
	NodeBinaryExpr
	NodeUnaryExpr
	NodeSubqueryExpr
	NodeWhere
	NodeJoin
	NodeGroupBy
	NodeOrderBy
	NodeLimit
)

type Node interface {
	Type() NodeType
	Accept(v Visitor) error
	Fingerprint() uint64
}
