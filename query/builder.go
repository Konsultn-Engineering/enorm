package query

import (
	"database/sql"
	"github.com/Konsultn-Engineering/enorm/visitor"
)

// Executor interface for *sql.DB and *sql.Tx compatibility
type Executor interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// BaseBuilder contains common functionality for all query builders
type BaseBuilder struct {
	tableName string
	visitor   *visitor.SQLVisitor
	executor  Executor
	errors    []error
}

// NewBaseBuilder creates a new base builder
func NewBaseBuilder(tableName string, visitor *visitor.SQLVisitor, executor Executor) *BaseBuilder {
	return &BaseBuilder{
		tableName: tableName,
		visitor:   visitor,
		executor:  executor,
		errors:    make([]error, 0),
	}
}

// TableName returns the table name
func (bb *BaseBuilder) TableName() string {
	return bb.tableName
}

// Visitor returns the SQL visitor
func (bb *BaseBuilder) Visitor() *visitor.SQLVisitor {
	return bb.visitor
}

// Executor returns the database executor
func (bb *BaseBuilder) Executor() Executor {
	return bb.executor
}

// AddError adds an error to the builder
func (bb *BaseBuilder) AddError(err error) {
	if err != nil {
		bb.errors = append(bb.errors, err)
	}
}

// HasErrors returns true if there are any errors
func (bb *BaseBuilder) HasErrors() bool {
	return len(bb.errors) > 0
}

// GetErrors returns all accumulated errors
func (bb *BaseBuilder) GetErrors() []error {
	return bb.errors
}

// GetFirstError returns the first error or nil
func (bb *BaseBuilder) GetFirstError() error {
	if len(bb.errors) > 0 {
		return bb.errors[0]
	}
	return nil
}

//// Builder factory functions
//func NewInsertBuilder(tableName string, visitor *visitor.SQLVisitor, executor Executor) *InsertBuilder {
//	return &InsertBuilder{
//		BaseBuilder: NewBaseBuilder(tableName, visitor, executor),
//		// Initialize insert-specific fields
//	}
//}
//
//func NewUpdateBuilder(tableName string, visitor *visitor.SQLVisitor, executor Executor) *UpdateBuilder {
//	return &UpdateBuilder{
//		BaseBuilder: NewBaseBuilder(tableName, visitor, executor),
//		// Initialize update-specific fields
//	}
//}
//
//func NewDeleteBuilder(tableName string, visitor *visitor.SQLVisitor, executor Executor) *DeleteBuilder {
//	return &DeleteBuilder{
//		BaseBuilder: NewBaseBuilder(tableName, visitor, executor),
//		// Initialize delete-specific fields
//	}
//}
//
//func NewRawBuilder(sql string, executor Executor, args ...interface{}) *RawBuilder {
//	return &RawBuilder{
//		sql:      sql,
//		args:     args,
//		executor: executor,
//	}
//}

// Transaction-aware builder constructors
//func NewSelectBuilderWithTx(schema, tableName string, visitor *visitor.SQLVisitor, tx *sql.Tx, exec Executor) *SelectBuilder {
//	return NewSelectBuilder(schema, tableName, visitor, exec)
//}

//func NewInsertBuilderWithTx(tableName string, visitor *visitor.SQLVisitor, tx *sql.Tx) *InsertBuilder {
//	return NewInsertBuilder(tableName, visitor, tx)
//}
//
//func NewUpdateBuilderWithTx(tableName string, visitor *visitor.SQLVisitor, tx *sql.Tx) *UpdateBuilder {
//	return NewUpdateBuilder(tableName, visitor, tx)
//}
//
//func NewDeleteBuilderWithTx(tableName string, visitor *visitor.SQLVisitor, tx *sql.Tx) *DeleteBuilder {
//	return NewDeleteBuilder(tableName, visitor, tx)
//}

// Common query result scanning utilities
func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

func scanRow(row *sql.Row) (map[string]interface{}, error) {
	// This is more complex as sql.Row doesn't expose column information
	// You might need to implement this differently based on your needs
	// For now, this is a placeholder
	return nil, nil
}
