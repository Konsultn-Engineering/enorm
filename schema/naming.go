package schema

import (
	"strings"
	"unicode"

	pluralizer "github.com/gertd/go-pluralize"
)

// Comprehensive naming utility system for database schema generation.
// Supports configurable naming conventions for both table and column names.

// pluralizeClient is a singleton instance for consistent pluralization behavior.
var pluralizeClient = pluralizer.NewClient()

// =========================================================================
// Core Interfaces
// =========================================================================

// NamingStrategy defines the complete naming configuration for database schema generation.
// Combines both column and table naming strategies with cardinality settings.
type NamingStrategy interface {
	ColumnNamingStrategy
	TableNamingStrategy
	CardinalityProvider
}

// ColumnNamingStrategy defines how Go field names are converted to database column names.
type ColumnNamingStrategy interface {
	// ColumnName converts a Go field name to a database column name.
	// Should return consistent results for the same input.
	ColumnName(fieldName string) string
}

// TableNamingStrategy defines how Go struct names are converted to database table names.
type TableNamingStrategy interface {
	// TableName converts a Go struct name to a database table name.
	// Should return consistent results for the same input.
	TableName(structName string) string
}

// CardinalityProvider defines whether table names should be pluralized.
type CardinalityProvider interface {
	// IsPlural returns true if table names should be pluralized.
	IsPlural() bool
}

// =========================================================================
// Column Naming Strategies
// =========================================================================

// ColumnNamingType represents different column naming conventions.
type ColumnNamingType int

const (
	ColumnSnakeCase  ColumnNamingType = iota // user_id, first_name, created_at
	ColumnCamelCase                          // userId, firstName, createdAt
	ColumnPascalCase                         // UserId, FirstName, CreatedAt
)

// columnNamingStrategy implements ColumnNamingStrategy for different naming conventions.
type columnNamingStrategy struct {
	namingType ColumnNamingType
}

// NewColumnNamingStrategy creates a new column naming strategy.
func NewColumnNamingStrategy(namingType ColumnNamingType) ColumnNamingStrategy {
	return &columnNamingStrategy{namingType: namingType}
}

// ColumnName converts field names according to the configured strategy.
func (c *columnNamingStrategy) ColumnName(fieldName string) string {
	switch c.namingType {
	case ColumnSnakeCase:
		return toSnakeCase(fieldName)
	case ColumnCamelCase:
		return toCamelCase(fieldName)
	case ColumnPascalCase:
		return toPascalCase(fieldName)
	default:
		return toSnakeCase(fieldName) // Default to snake_case
	}
}

// =========================================================================
// Table Naming Strategies
// =========================================================================

// TableNamingType represents different table naming conventions.
type TableNamingType int

const (
	TableSnakeCaseSingular  TableNamingType = iota // user, blog_post, oauth2_token
	TableSnakeCasePlural                           // users, blog_posts, oauth2_tokens
	TableCamelCaseSingular                         // user, blogPost, oauth2Token
	TableCamelCasePlural                           // users, blogPosts, oauth2Tokens
	TablePascalCaseSingular                        // User, BlogPost, Oauth2Token
	TablePascalCasePlural                          // Users, BlogPosts, Oauth2Tokens
)

// tableNamingStrategy implements TableNamingStrategy for different naming conventions.
type tableNamingStrategy struct {
	namingType TableNamingType
}

// NewTableNamingStrategy creates a new table naming strategy.
func NewTableNamingStrategy(namingType TableNamingType) TableNamingStrategy {
	return &tableNamingStrategy{namingType: namingType}
}

// TableName converts struct names according to the configured strategy.
func (t *tableNamingStrategy) TableName(structName string) string {
	switch t.namingType {
	case TableSnakeCaseSingular:
		return toSnakeCase(structName)
	case TableSnakeCasePlural:
		snake := toSnakeCase(structName)
		return pluralize(snake)
	case TableCamelCaseSingular:
		return toCamelCase(structName)
	case TableCamelCasePlural:
		camel := toCamelCase(structName)
		return pluralize(camel)
	case TablePascalCaseSingular:
		return toPascalCase(structName)
	case TablePascalCasePlural:
		pascal := toPascalCase(structName)
		return pluralize(pascal)
	default:
		// Default to snake_case plural (most common)
		snake := toSnakeCase(structName)
		return pluralize(snake)
	}
}

// IsPlural returns whether this strategy produces plural table names.
func (t *tableNamingStrategy) IsPlural() bool {
	switch t.namingType {
	case TableSnakeCasePlural, TableCamelCasePlural, TablePascalCasePlural:
		return true
	default:
		return false
	}
}

// =========================================================================
// Cardinality Provider
// =========================================================================

// cardinalityProvider implements CardinalityProvider with configurable pluralization.
type cardinalityProvider struct {
	plural bool
}

// NewCardinalityProvider creates a new cardinality provider.
func NewCardinalityProvider(plural bool) CardinalityProvider {
	return &cardinalityProvider{plural: plural}
}

// IsPlural returns the configured pluralization setting.
func (c *cardinalityProvider) IsPlural() bool {
	return c.plural
}

// =========================================================================
// Combined Naming Strategies
// =========================================================================

// CombinedNamingStrategy combines column and table naming strategies.
type CombinedNamingStrategy struct {
	ColumnNamingStrategy
	TableNamingStrategy
	CardinalityProvider
}

// NewCombinedNamingStrategy creates a complete naming strategy.
func NewCombinedNamingStrategy(
	columnNamingStrategy ColumnNamingStrategy,
	tableNamingStrategy TableNamingStrategy,
	cardinalityProvider CardinalityProvider,
) NamingStrategy {
	return &CombinedNamingStrategy{
		ColumnNamingStrategy: columnNamingStrategy,
		TableNamingStrategy:  tableNamingStrategy,
		CardinalityProvider:  cardinalityProvider,
	}
}

// =========================================================================
// Predefined Strategies (Common Combinations)
// =========================================================================

// DefaultNamingStrategy returns the default snake_case strategy with plural tables.
func DefaultNamingStrategy() NamingStrategy {
	return NewCombinedNamingStrategy(
		NewColumnNamingStrategy(ColumnSnakeCase),
		NewTableNamingStrategy(TableSnakeCasePlural),
		NewCardinalityProvider(true),
	)
}

// JSONAPIStrategy returns camelCase columns with plural snake_case tables.
// Common for JSON APIs with traditional database tables.
func JSONAPIStrategy() NamingStrategy {
	return NewCombinedNamingStrategy(
		NewColumnNamingStrategy(ColumnCamelCase),
		NewTableNamingStrategy(TableSnakeCasePlural),
		NewCardinalityProvider(true),
	)
}

// NoSQLStrategy returns camelCase for both columns and tables (MongoDB style).
func NoSQLStrategy() NamingStrategy {
	return NewCombinedNamingStrategy(
		NewColumnNamingStrategy(ColumnCamelCase),
		NewTableNamingStrategy(TableCamelCasePlural),
		NewCardinalityProvider(true),
	)
}

// GraphQLStrategy returns PascalCase for both columns and tables.
func GraphQLStrategy() NamingStrategy {
	return NewCombinedNamingStrategy(
		NewColumnNamingStrategy(ColumnPascalCase),
		NewTableNamingStrategy(TablePascalCasePlural),
		NewCardinalityProvider(true),
	)
}

// =========================================================================
// Core Conversion Functions
// =========================================================================

// toSnakeCase converts any naming convention to snake_case.
// Handles complex cases including acronyms, numbers, and edge cases.
func toSnakeCase(name string) string {
	if name == "" {
		return ""
	}

	// Handle special common cases for performance
	switch name {
	case "ID":
		return "id"
	case "UUID":
		return "uuid"
	case "URL":
		return "url"
	case "HTTP":
		return "http"
	case "HTTPS":
		return "https"
	case "API":
		return "api"
	case "JSON":
		return "json"
	case "XML":
		return "xml"
	case "SQL":
		return "sql"
	case "HTML":
		return "html"
	case "CSS":
		return "css"
	case "JWT":
		return "jwt"
	case "OAuth":
		return "o_auth"
	case "OAuth2":
		return "o_auth2"
	}

	// If already snake_case (contains underscores and no uppercase), return as-is
	if strings.Contains(name, "_") && !hasUpperCase(name) {
		return strings.ToLower(name)
	}

	var result strings.Builder
	result.Grow(len(name) + 10) // Pre-allocate with some extra space for underscores

	runes := []rune(name)

	for i, r := range runes {
		// Convert to lowercase for output
		lower := unicode.ToLower(r)

		// Decide whether to add underscore before this character
		needsUnderscore := false

		if i > 0 && unicode.IsUpper(r) {
			prev := runes[i-1]

			// Add underscore before uppercase letters in these cases:
			// 1. Previous char is lowercase or digit: aB -> a_b, a1B -> a1_b
			// 2. Previous char is uppercase, but next char is lowercase: ABc -> a_bc
			if unicode.IsLower(prev) || unicode.IsDigit(prev) {
				needsUnderscore = true
			} else if unicode.IsUpper(prev) && i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
				needsUnderscore = true
			}
		}

		// Add underscore if needed
		if needsUnderscore {
			result.WriteByte('_')
		}

		result.WriteRune(lower)
	}

	return result.String()
}

// toCamelCase converts any naming convention to camelCase.
func toCamelCase(name string) string {
	if name == "" {
		return ""
	}

	// Convert from other formats first
	snake := toSnakeCase(name)

	// If no underscores, just return with first letter lowercase
	if !strings.Contains(snake, "_") {
		if len(snake) == 1 {
			return strings.ToLower(snake)
		}
		return strings.ToLower(snake[:1]) + snake[1:]
	}

	parts := strings.Split(snake, "_")
	if len(parts) == 0 {
		return ""
	}

	var result strings.Builder
	result.Grow(len(name)) // Approximate final size

	// First part stays lowercase
	result.WriteString(strings.ToLower(parts[0]))

	// Remaining parts get title cased
	for _, part := range parts[1:] {
		if part != "" {
			result.WriteString(strings.ToLower(part))
		}
	}

	return result.String()
}

// toPascalCase converts any naming convention to PascalCase.
func toPascalCase(name string) string {
	if name == "" {
		return ""
	}

	// Convert from other formats first
	snake := toSnakeCase(name)

	// If no underscores, capitalize first letter
	if !strings.Contains(snake, "_") {
		if len(snake) == 1 {
			return strings.ToUpper(snake)
		}
		return strings.ToUpper(snake[:1]) + snake[1:]
	}

	parts := strings.Split(snake, "_")
	if len(parts) == 0 {
		return ""
	}

	var result strings.Builder
	result.Grow(len(name)) // Approximate final size

	// All parts get title cased
	for _, part := range parts {
		if part != "" {
			result.WriteString(strings.ToLower(part))
		}
	}

	return result.String()
}

// =========================================================================
// Pluralization Functions
// =========================================================================

// pluralize converts singular nouns to their plural forms.
func pluralize(name string) string {
	if name == "" {
		return ""
	}

	// Handle some common special cases for performance
	switch strings.ToLower(name) {
	case "person":
		return "people"
	case "child":
		return "children"
	case "mouse":
		return "mice"
	case "goose":
		return "geese"
	case "tooth":
		return "teeth"
	case "foot":
		return "feet"
	case "man":
		return "men"
	case "woman":
		return "women"
	case "datum":
		return "data"
	case "medium":
		return "media"
	case "criterion":
		return "criteria"
	case "phenomenon":
		return "phenomena"
	}

	// Use the pluralizer library for general cases
	plural := pluralizeClient.Pluralize(name, 2, false)
	return preserveCase(name, plural)
}

// singularize converts plural nouns to their singular forms.
func singularize(name string) string {
	if name == "" {
		return ""
	}

	// Handle special cases
	switch strings.ToLower(name) {
	case "people":
		return "person"
	case "children":
		return "child"
	case "mice":
		return "mouse"
	case "geese":
		return "goose"
	case "teeth":
		return "tooth"
	case "feet":
		return "foot"
	case "men":
		return "man"
	case "women":
		return "woman"
	case "data":
		return "datum"
	case "media":
		return "medium"
	case "criteria":
		return "criterion"
	case "phenomena":
		return "phenomenon"
	}

	// Use the pluralizer library
	singular := pluralizeClient.Pluralize(name, 1, false)
	return preserveCase(name, singular)
}

// =========================================================================
// Utility Functions
// =========================================================================

// hasUpperCase returns true if the string contains any uppercase letters.
func hasUpperCase(s string) bool {
	for _, r := range s {
		if unicode.IsUpper(r) {
			return true
		}
	}
	return false
}

// preserveCase preserves the case pattern of the original string in the result.
func preserveCase(original, result string) string {
	if original == "" || result == "" {
		return result
	}

	// If original is all lowercase, return lowercase result
	if strings.ToLower(original) == original {
		return strings.ToLower(result)
	}

	// If original is all uppercase, return uppercase result
	if strings.ToUpper(original) == original {
		return strings.ToUpper(result)
	}

	// If original starts with uppercase, capitalize result
	if unicode.IsUpper(rune(original[0])) {
		if len(result) == 1 {
			return strings.ToUpper(result)
		}
		return strings.ToUpper(result[:1]) + strings.ToLower(result[1:])
	}

	// Default to lowercase
	return strings.ToLower(result)
}

// =========================================================================
// Validation Functions
// =========================================================================

// isSnakeCase returns true if the string follows snake_case convention.
func isSnakeCase(name string) bool {
	if name == "" {
		return true
	}

	// Must be all lowercase with optional underscores and digits
	for _, r := range name {
		if !(unicode.IsLower(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}

	// Must not start or end with underscore
	return !strings.HasPrefix(name, "_") && !strings.HasSuffix(name, "_")
}

// isCamelCase returns true if the string follows camelCase convention.
func isCamelCase(name string) bool {
	if name == "" {
		return true
	}

	// Must start with lowercase letter
	if !unicode.IsLower(rune(name[0])) {
		return false
	}

	// Must not contain underscores
	return !strings.Contains(name, "_")
}

// isPascalCase returns true if the string follows PascalCase convention.
func isPascalCase(name string) bool {
	if name == "" {
		return true
	}

	// Must start with uppercase letter
	if !unicode.IsUpper(rune(name[0])) {
		return false
	}

	// Must not contain underscores
	return !strings.Contains(name, "_")
}
