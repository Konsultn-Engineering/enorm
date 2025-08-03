package query

//
//import "context"
//
//// Template interface for parameter substitution
//type Template interface {
//	WithParams(values ...any) ExecutableTemplate
//	WithColumns(columns ...string) Template
//	WithTable(table string) Template
//	Clone() Template
//}
//
//// Executable template with parameters bound
//type ExecutableTemplate interface {
//	ToSQL() (string, []any, error)
//	Execute(ctx context.Context) ([]any, error)
//	ExecuteOne(ctx context.Context) (any, error)
//}
//
//// Concrete implementation
//type FragmentTemplate struct {
//	composer    *TemplateComposer
//	table       string
//	columns     []string
//	paramValues []any
//	paramCount  int
//}
//
//func (t *FragmentTemplate) WithParams(values ...any) ExecutableTemplate {
//	return &ExecutableFragmentTemplate{
//		template: t,
//		params:   values,
//	}
//}
//
//func (t *FragmentTemplate) WithColumns(columns ...string) Template {
//	newTemplate := t.Clone().(*FragmentTemplate)
//	newTemplate.columns = columns
//	return newTemplate
//}
//
//func (t *FragmentTemplate) WithTable(table string) Template {
//	newTemplate := t.Clone().(*FragmentTemplate)
//	newTemplate.table = table
//	return newTemplate
//}
