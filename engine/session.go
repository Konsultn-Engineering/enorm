package engine

import (
	"fmt"
	"konsultn-api/pkg/sqlorm/builder"
	"konsultn-api/pkg/sqlorm/schema"
	"reflect"
)

type Session struct {
	engine *Engine
	where  string
	args   []any

	selectCols  []string
	joinClauses []string
	orderBy     string
	limit       *int
	offset      *int
}

func (s *Session) Select(cols ...string) *Session {
	s.selectCols = cols
	return s
}

func (s *Session) Join(clause string) *Session {
	s.joinClauses = append(s.joinClauses, clause)
	return s
}

func (s *Session) Order(by string) *Session {
	s.orderBy = by
	return s
}

func (s *Session) Limit(n int) *Session {
	s.limit = &n
	return s
}

func (s *Session) Offset(n int) *Session {
	s.offset = &n
	return s
}

func (s *Session) Find(dest any) error {
	// Validate destination
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("Find expects pointer to slice")
	}
	elemType := destVal.Elem().Type().Elem()

	// Get model metadata
	meta, err := schema.GetModelMeta(elemType)
	if err != nil {
		return err
	}

	// SELECT columns
	columns := meta.Columns()
	if len(s.selectCols) > 0 {
		columns = s.selectCols
	}

	// Build SQL query
	qb := builder.NewSelect(meta.TableName, columns)

	if s.where != "" {
		qb = qb.Where(s.where, s.args...)
	}
	for _, join := range s.joinClauses {
		qb = qb.Join(join)
	}
	if s.orderBy != "" {
		qb = qb.Order(s.orderBy)
	}
	if s.limit != nil {
		qb = qb.Limit(*s.limit)
	}
	if s.offset != nil {
		qb = qb.Offset(*s.offset)
	}

	queryStr, queryArgs := qb.Build()

	// Execute
	rows, err := s.engine.db.Query(queryStr, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Scan rows
	slice := reflect.MakeSlice(destVal.Elem().Type(), 0, 10)
	for rows.Next() {
		elem := reflect.New(elemType).Elem()
		if err := schema.ScanInto(meta, elem, rows); err != nil {
			return err
		}
		slice = reflect.Append(slice, elem)
	}
	destVal.Elem().Set(slice)
	return nil
}
