package context

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/schema"
)

type ConnectionContext interface {
	GetVisitor() ast.Visitor
	GetSchema() *schema.Context
}
