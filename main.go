package main

import (
	"github.com/Konsultn-Engineering/enorm/ast"
	_ "github.com/Konsultn-Engineering/enorm/providers/postgres"
	"time"
)

type User struct {
	ID        uint64
	FirstName string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func test() {
	_ = ast.SelectStmt{
		Columns: []ast.Node{
			&ast.Column{Name: "id"},
			&ast.Column{Name: "name"},
		},
		From: &ast.Table{Name: "users"},
		Where: &ast.WhereClause{
			Condition: &ast.BinaryExpr{
				Operator: "OR",
				Left: &ast.BinaryExpr{
					Operator: "AND",
					Left: &ast.BinaryExpr{
						Operator: "AND",
						Left: &ast.BinaryExpr{
							Left:     &ast.Column{Name: "a"},
							Operator: "=",
							Right:    &ast.Value{Val: 1, ValueType: ast.ValueInt},
						},
						Right: &ast.BinaryExpr{
							Left:     &ast.Column{Name: "b"},
							Operator: "=",
							Right:    &ast.Value{Val: 2, ValueType: ast.ValueInt},
						},
					},
					Right: &ast.BinaryExpr{
						Left:     &ast.Column{Name: "c"},
						Operator: "=",
						Right:    &ast.Value{Val: 3, ValueType: ast.ValueInt},
					},
				},
				Right: &ast.BinaryExpr{
					Left:     &ast.Column{Name: "d"},
					Operator: "=",
					Right:    &ast.Value{Val: "x", ValueType: ast.ValueString},
				},
			},
		},
	}
	//selectStmt := ast.SelectStmt{
	//	Columns: []ast.Node{
	//		&ast.Column{Name: "id"},
	//		&ast.Column{Name: "name"},
	//	},
	//	From: &ast.Table{Name: "users"},
	//	Where: &ast.WhereClause{
	//		Condition: &ast.BinaryExpr{
	//			Left:     &ast.Column{Name: "id"},
	//			Operator: "IN",
	//			Right: &ast.SubqueryExpr{
	//				Stmt: &ast.SelectStmt{
	//					Columns: []ast.Node{
	//						&ast.Column{Name: "user_id"},
	//					},
	//					From: &ast.Table{Name: "posts"},
	//					Where: &ast.WhereClause{
	//						Condition: &ast.BinaryExpr{
	//							Left:     &ast.Column{Name: "published"},
	//							Operator: "=",
	//							Right:    &ast.Value{Val: true, ValueType: ast.ValueBool},
	//						},
	//					},
	//				},
	//			},
	//		},
	//	},
	//	GroupBy: &ast.GroupByClause{
	//		Exprs: []ast.Node{
	//			&ast.Column{Name: "id"},
	//			&ast.Column{Name: "name"},
	//		},
	//	},
	//}

	//c := cache.NewQueryCache() // your real cache instance
	//d := dialect.NewTiDBDialect()
	//v := visitor.NewSQLVisitor(d, c)
	//
	//fp := selectStmt.Fingerprint()
	//
	//// First access: should MISS
	//if cached, ok := c.GetSQL(fp); ok {
	//	log.Println("CACHE HIT:", cached.SQL)
	//} else {
	//	sql, args, err := v.Build(&selectStmt)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	c.SetSQL(fp, &cache.CachedQuery{SQL: sql})
	//	log.Println("CACHE MISS -> COMPUTED:", sql)
	//	log.Println("ARGS:", args)
	//}
	//
	//// Second access: should HIT
	//if cached, ok := c.GetSQL(fp); ok {
	//	log.Println("RECONFIRM HIT:", cached.SQL, cached.ArgsOrder)
	//} else {
	//	log.Println("UNEXPECTED MISS")
	//}

}
