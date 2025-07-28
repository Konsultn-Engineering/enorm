package engine

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Konsultn-Engineering/enorm/ast"
	"github.com/Konsultn-Engineering/enorm/cache"
	"github.com/Konsultn-Engineering/enorm/dialect"
	"github.com/Konsultn-Engineering/enorm/schema"
	"github.com/Konsultn-Engineering/enorm/visitor"
	"reflect"
	"sync"
)

type Engine struct {
	db      *sql.DB
	visitor *visitor.SQLVisitor

	selectCache sync.Map  // map[reflect.Type]string
	addrsPool   sync.Pool // []any
}

func New(db *sql.DB) *Engine {
	v := visitor.NewSQLVisitor(dialect.Postgres{}, cache.NewQueryCache())
	return &Engine{
		db:      db,
		visitor: v,
		addrsPool: sync.Pool{
			New: func() any {
				return make([]any, 0, 32)
			},
		},
	}
}

type Session struct {
	engine *Engine
	cols   []string
	scanFn schema.ScannerFunc
	meta   *schema.EntityMeta
}

func (e *Engine) Select(cols ...string) *Session {
	return &Session{
		engine: e,
		cols:   cols,
	}
}
func (s *Session) FindOne(ctx context.Context, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("FindOne expects non-nil pointer to struct")
	}
	elem := val.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("FindOne expects pointer to struct, got %s", elem.Kind())
	}

	typ := elem.Type()
	meta, err := schema.Introspect(typ)
	if err != nil {
		return err
	}

	if meta.ScannerFn == nil {
		return fmt.Errorf("no scanner registered for type %s", typ.Name())
	}

	// build AST
	selectStmt := &ast.SelectStmt{
		Columns: make([]ast.Node, len(s.cols)),
		From:    &ast.Table{Name: meta.Plural},
		Limit:   &ast.LimitClause{Count: ptr(1)},
	}
	for i, name := range s.cols {
		selectStmt.Columns[i] = &ast.Column{Name: name}
	}

	query, _, err := s.engine.visitor.Build(selectStmt)
	if err != nil {
		return err
	}

	// execute
	rows, err := s.engine.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	if err := meta.ScannerFn(dest, reg); err != nil {
		return err
	}

	// build address slice from binds
	binds := schema.
	addrs := s.engine.getAddrs(len(s.cols))

	for i, col := range s.cols {
		fn, ok := binds[col]
		if !ok {
			return fmt.Errorf("field %q not bound in scanner", col)
		}
		// simulate target allocation
		// we allocate ptr, then set to struct field in-place after Scan
		var tmp any
		addrs[i] = &tmp
	}

	if err := rows.Scan(addrs...); err != nil {
		s.engine.putAddrs(addrs)
		return err
	}

	// apply values via bind funcs
	for i, col := range s.cols {
		ptr := addrs[i].(*any)
		if fn, ok := binds[col]; ok {
			fn(dest, *ptr)
		}
	}

	s.engine.putAddrs(addrs)
	return nil
}

func (e *Engine) FindOne(ctx context.Context, dest any) error {
	val := reflect.ValueOf(dest)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("FindOne expects non-nil pointer to struct")
	}
	elem := val.Elem()
	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("FindOne expects pointer to struct, got %s", elem.Kind())
	}

	typ := elem.Type()
	meta, err := schema.Introspect(typ)
	if err != nil {
		return err
	}

	var query string
	if cached, ok := e.selectCache.Load(typ); ok {
		query = cached.(string)
	} else {
		selectStmt := &ast.SelectStmt{
			Columns: make([]ast.Node, len(meta.Fields)),
			From:    &ast.Table{Name: meta.Plural},
			Limit:   &ast.LimitClause{Count: ptr(1)},
		}
		for i, f := range meta.Fields {
			selectStmt.Columns[i] = &ast.Column{Name: f.DBName}
		}
		sql, _, err := e.visitor.Build(selectStmt)
		if err != nil {
			return err
		}
		query = sql
		e.selectCache.Store(typ, query)
	}

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	addrs := e.getAddrs(len(meta.Fields))
	for i, f := range meta.Fields {
		addrs[i] = elem.FieldByIndex(f.Index).Addr().Interface()
	}

	err = rows.Scan(addrs...)
	e.putAddrs(addrs)
	return err
}

func (e *Engine) getAddrs(n int) []any {
	a := e.addrsPool.Get().([]any)
	if cap(a) < n {
		return make([]any, n)
	}
	return a[:n]
}

func (e *Engine) putAddrs(a []any) {
	e.addrsPool.Put(a)
}

func ptr[T any](v T) *T {
	return &v
}
