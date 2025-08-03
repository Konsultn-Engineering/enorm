package query

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/ast"
	"sync"
)

// FragmentType for organizing fragment categories
type FragmentType int

const (
	FragmentColumns FragmentType = iota
	FragmentWhere
	FragmentOrderBy
	FragmentJoin
	FragmentLimit
	FragmentValues
)

// Fragment definition with metadata
type Fragment struct {
	template     ast.Node // Template node to clone
	paramSlots   int      // Number of parameters this fragment uses
	isRepeatable bool     // Can this fragment be repeated N times?
}

// Fragment registry with enhanced patterns
var Fragments = map[FragmentType]map[string]*Fragment{
	FragmentColumns: {
		"column":       {template: &ast.Column{}, paramSlots: 0, isRepeatable: true},
		"column_id":    {template: &ast.Column{Name: "id"}, paramSlots: 0, isRepeatable: false},
		"column_all":   {template: &ast.Column{Name: "*"}, paramSlots: 0, isRepeatable: false},
		"column_count": {template: &ast.Function{Name: "COUNT", Args: []ast.Node{&ast.Column{Name: "*"}}}, paramSlots: 0, isRepeatable: false},
	},

	FragmentWhere: {
		"eq":      {template: &ast.BinaryExpr{Operator: "="}, paramSlots: 1, isRepeatable: true},
		"in":      {template: &ast.BinaryExpr{Operator: "IN"}, paramSlots: -1, isRepeatable: false}, // Variable params
		"like":    {template: &ast.BinaryExpr{Operator: "LIKE"}, paramSlots: 1, isRepeatable: true},
		"is_null": {template: &ast.UnaryExpr{Operator: "IS NULL"}, paramSlots: 0, isRepeatable: true},
		"between": {template: &ast.BinaryExpr{Operator: "BETWEEN"}, paramSlots: 2, isRepeatable: true},
		"and":     {template: &ast.BinaryExpr{Operator: "AND"}, paramSlots: 0, isRepeatable: false}, // Connector
		"or":      {template: &ast.BinaryExpr{Operator: "OR"}, paramSlots: 0, isRepeatable: false},  // Connector
	},

	FragmentOrderBy: {
		"asc":          {template: &ast.OrderByClause{Desc: false}, paramSlots: 0, isRepeatable: true},
		"desc":         {template: &ast.OrderByClause{Desc: true}, paramSlots: 0, isRepeatable: true},
		"id_asc":       {template: &ast.OrderByClause{Expr: &ast.Column{Name: "id"}, Desc: false}, paramSlots: 0, isRepeatable: false},
		"created_desc": {template: &ast.OrderByClause{Expr: &ast.Column{Name: "created_at"}, Desc: true}, paramSlots: 0, isRepeatable: false},
	},

	FragmentLimit: {
		"limit":        {template: &ast.LimitClause{}, paramSlots: 1, isRepeatable: false},
		"limit_offset": {template: &ast.LimitClause{}, paramSlots: 2, isRepeatable: false},
		"limit_1":      {template: &ast.LimitClause{Count: &[]int{1}[0]}, paramSlots: 0, isRepeatable: false},
	},

	FragmentValues: {
		"value": {template: &ast.Value{}, paramSlots: 1, isRepeatable: true},
		"array": {template: &ast.Array{}, paramSlots: -1, isRepeatable: false}, // Variable values
	},
}

// Pool for node slices to avoid allocations
var nodeSlicePool = sync.Pool{
	New: func() any {
		return make([]ast.Node, 0, 8)
	},
}

// Enhanced node generation with smart cloning
func Nodes(fragmentType FragmentType, fragmentKey string, count int, options ...NodeOption) []ast.Node {
	fragment, exists := Fragments[fragmentType][fragmentKey]
	if !exists {
		return nil
	}

	// Validation
	if !fragment.isRepeatable && count > 1 {
		count = 1 // Non-repeatable fragments can only generate 1 node
	}

	// Get pooled slice
	nodes := nodeSlicePool.Get().([]ast.Node)
	defer nodeSlicePool.Put(nodes[:0])

	// Ensure capacity
	if cap(nodes) < count {
		nodes = make([]ast.Node, 0, count)
	}

	// Generate nodes
	for i := 0; i < count; i++ {
		node := cloneNode(fragment.template)

		// Apply options to customize the node
		for _, option := range options {
			node = option(node, i)
		}

		nodes = append(nodes, node)
	}

	// Return copy to prevent pool pollution
	result := make([]ast.Node, len(nodes))
	copy(result, nodes)
	return result
}

// Node customization options
type NodeOption func(ast.Node, int) ast.Node

// Common node options
func WithColumn(name string) NodeOption {
	return func(node ast.Node, index int) ast.Node {
		switch n := node.(type) {
		case *ast.Column:
			n.Name = name
		case *ast.BinaryExpr:
			if n.Left == nil {
				n.Left = &ast.Column{Name: name}
			}
		case *ast.OrderByClause:
			n.Expr = &ast.Column{Name: name}
		}
		return node
	}
}

func WithValue(value any) NodeOption {
	return func(node ast.Node, index int) ast.Node {
		switch n := node.(type) {
		case *ast.Value:
			n.Val = value
		case *ast.BinaryExpr:
			if n.Right == nil {
				n.Right = &ast.Value{Val: value}
			}
		}
		return node
	}
}

func WithIndexedColumn(baseName string) NodeOption {
	return func(node ast.Node, index int) ast.Node {
		columnName := baseName
		if index > 0 {
			columnName = fmt.Sprintf("%s_%d", baseName, index+1)
		}
		return WithColumn(columnName)(node, index)
	}
}

// Smart node cloning
func cloneNode(template ast.Node) ast.Node {
	switch n := template.(type) {
	case *ast.Column:
		return &ast.Column{
			Table: n.Table,
			Name:  n.Name,
			Alias: n.Alias,
		}
	case *ast.BinaryExpr:
		return &ast.BinaryExpr{
			Left:     cloneIfNotNil(n.Left),
			Operator: n.Operator,
			Right:    cloneIfNotNil(n.Right),
		}
	case *ast.OrderByClause:
		return &ast.OrderByClause{
			Expr: cloneIfNotNil(n.Expr),
			Desc: n.Desc,
		}
	case *ast.Value:
		return &ast.Value{Val: n.Val}
	case *ast.LimitClause:
		return &ast.LimitClause{
			Count:  n.Count,
			Offset: n.Offset,
		}
	default:
		// Fallback for other node types
		return template
	}
}

func cloneIfNotNil(node ast.Node) ast.Node {
	if node == nil {
		return nil
	}
	return cloneNode(node)
}
