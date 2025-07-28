package dialect

type Dialect interface {
	QuoteIdentifier(name string) string
	Placeholder(n int) string
	RenderValue(v any) string
	SupportsVector() bool
}
