package schema

import (
	"database/sql"
	"encoding/json"
	"sync"
	"time"
)

var setterCreators = sync.Map{}

func init() {
	// Register essential types
	RegisterBasicType[string]()
	RegisterBasicType[bool]()
	RegisterBasicType[int64]()
	RegisterBasicType[uint64]()
	RegisterBasicType[float64]()
	RegisterBasicType[time.Time]()
	RegisterBasicType[[]byte]()
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
	namingStrategy NamingStrategy
	tagName        string
	caseSensitive  bool
}

var schemaContext = &Context{
	namingStrategy: DefaultNamingStrategy(),
	tagName:        "db",
	caseSensitive:  false,
}

// SetGlobalSchemaContext updates the global schema configuration
// Called by enorm during initialization
func SetGlobalSchemaContext(namingStrategy NamingStrategy, tagName string, caseSensitive bool) {
	schemaContext = &Context{
		namingStrategy: namingStrategy,
		tagName:        tagName,
		caseSensitive:  caseSensitive,
	}
}
