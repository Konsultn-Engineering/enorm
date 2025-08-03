package schema

import (
	"database/sql"
	"encoding/json"
	lru "github.com/hashicorp/golang-lru/v2"
	"reflect"
	"sync"
)

var setterCreators = sync.Map{}

func init() {
	// Register essential types
	RegisterBasicType[string]()
	RegisterBasicType[bool]()
	RegisterBasicType[json.RawMessage]()

	// Vector types
	RegisterVectorType[[]float32](0)
	RegisterVectorType[[]float64](0)

	// Nullable SQL types
	RegisterBasicType[sql.NullString]()
	RegisterBasicType[sql.NullTime]()
	RegisterBasicType[sql.NullBool]()
	RegisterBasicType[sql.NullInt64]()
	RegisterBasicType[sql.NullFloat64]()

	// Common vector dimensions for AI/ML
	RegisterVectorType[[384]float32](384)   // BERT base
	RegisterVectorType[[768]float32](768)   // BERT large
	RegisterVectorType[[1536]float32](1536) // OpenAI embeddings
}

type Context struct {
	// Configuration
	namingStrategy NamingStrategy
	tagName        string
	caseSensitive  bool
	guaranteeTypes bool
	validateInDev  bool

	// Performance optimization storage
	entityCache      *lru.Cache[reflect.Type, *EntityMeta]
	precompiledTypes map[reflect.Type]*EntityMeta
	precompiledMu    sync.RWMutex

	// Cache configuration
	cacheSize int
	onEvict   func(reflect.Type, *EntityMeta)
}

type Option func(*Context)

// WithNamingStrategy sets the naming strategy for database column mapping
func WithNamingStrategy(strategy NamingStrategy) Option {
	return func(ctx *Context) { ctx.namingStrategy = strategy }
}

// WithTagName sets the struct tag name to use for database field mapping
func WithTagName(tagName string) Option {
	return func(ctx *Context) { ctx.tagName = tagName }
}

// WithCaseSensitive enables or disables case-sensitive field matching
func WithCaseSensitive(sensitive bool) Option {
	return func(ctx *Context) { ctx.caseSensitive = sensitive }
}

// WithGuaranteeTypes enables unsafe direct type casting for maximum performance.
// When enabled, skips all type conversion and validation - user guarantees
// that database values exactly match Go struct field types.
func WithGuaranteeTypes(enabled bool) Option {
	return func(ctx *Context) { ctx.guaranteeTypes = enabled }
}

// WithValidateInDev enables type validation during development.
// Only has effect when GuaranteeTypes is also enabled.
func WithValidateInDev(enabled bool) Option {
	return func(ctx *Context) { ctx.validateInDev = enabled }
}

// WithCacheSize sets the LRU cache size for struct metadata
func WithCacheSize(size int) Option {
	return func(ctx *Context) { ctx.cacheSize = size }
}

// WithEvictionCallback sets a callback function for cache eviction events
func WithEvictionCallback(onEvict func(reflect.Type, *EntityMeta)) Option {
	return func(ctx *Context) { ctx.onEvict = onEvict }
}

// New creates a schema with configuration
// Called by enorm during initialization
func New(options ...Option) *Context {
	ctx := &Context{
		// Default configuration
		namingStrategy: DefaultNamingStrategy(),
		tagName:        "db",
		caseSensitive:  false,
		guaranteeTypes: false,
		validateInDev:  true,
		cacheSize:      256,

		// Initialize storage
		precompiledTypes: make(map[reflect.Type]*EntityMeta, 64),
	}

	// Apply options
	for _, opt := range options {
		opt(ctx)
	}

	// Initialize cache
	//ctx.initializeCache()

	return ctx
}
