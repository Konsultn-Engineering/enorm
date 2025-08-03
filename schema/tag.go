package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// ParsedTag represents comprehensive parsed enorm struct tag configuration.
// Contains all database mapping, validation, and auto-generation settings
// extracted from struct field tags for high-performance field processing.
type ParsedTag struct {
	// Core database mapping
	ColumnName string // Database column name (explicit or derived from field name)
	Skip       bool   // Skip this field entirely (db:"-")
	Type       string

	// Column constraints and properties
	Null       bool   // Allow NULL values explicitly
	NotNull    bool   // Enforce NOT NULL constraint
	Default    string // Default value expression
	Primary    bool   // Primary key constraint
	Unique     string // Unique constraint (empty for simple, name for named constraint)
	Index      string // Index directive (empty for simple, name for named index)
	ForeignKey string // Foreign key reference (table.column format)

	// Validation constraints (most commonly used)
	MinLength *int     // Minimum string/slice length
	MaxLength *int     // Maximum string/slice length
	Enum      []string // Enumerated allowed values

	// Automatic timestamp management
	AutoNowAdd bool // Set to current time on INSERT only
	AutoNow    bool // Set to current time on INSERT and UPDATE

	// ID generation configuration
	AutoGenerate bool   // Enable automatic ID generation
	Generator    string // Specific generator name (uuid, ulid, snowflake, nanoid)
}

// TagParser handles efficient parsing and caching of enorm struct tags.
// Implements smart caching to avoid repeated string parsing overhead
// and provides pluggable naming strategies for different conventions.
type TagParser struct {
	namingStrategy NamingStrategy        // Field name to column name conversion
	cache          map[string]*ParsedTag // Parsed tag cache for performance
	cacheMu        sync.RWMutex          // Protect cache for concurrent access
}

// NewTagParser creates a new tag parser with snake_case naming strategy.
// Uses snake_case as the default since it's the most common database convention.
//
// For custom naming strategies:
//
//	parser := NewTagParser()
//	parser.SetNamingStrategy(PascalCaseStrategy{})
func NewTagParser(namingStrategy NamingStrategy) *TagParser {
	return &TagParser{
		namingStrategy: namingStrategy,
		cache:          make(map[string]*ParsedTag, 128), // Pre-sized for typical applications
	}
}

// SetNamingStrategy allows customization of field name to column name conversion.
// Must be called before any ParseTag operations for consistent results.
//
// Parameters:
//   - strategy: Implementation of NamingStrategy interface
func (p *TagParser) SetNamingStrategy(strategy NamingStrategy) {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()

	p.namingStrategy = strategy
	// Clear cache since naming strategy changed
	clear(p.cache)
}

// ParseTag parses an enorm struct tag and returns comprehensive configuration.
// Implements smart caching to avoid repeated parsing of identical tags.
//
// Supported tag syntax:
//
//	`db:"column_name"`                    // Basic column mapping
//	`db:"column:custom_name"`             // Explicit column name
//	`db:"primary;unique;not null"`        // Multiple constraints
//	`db:"type:varchar(255);default:''"`   // Type override with default
//	`db:"auto_generate;generator:uuid"`   // ID generation
//	`db:"-"`                              // Skip field entirely
//
// Parameters:
//   - fieldName: Go struct field name
//   - tag: Complete reflect.StructTag from the field
//
// Returns: Parsed configuration or error for invalid syntax
func (p *TagParser) ParseTag(fieldName string, tag reflect.StructTag) (*ParsedTag, error) {
	tagValue := tag.Get("db")

	// Handle fields without db tags (use default column naming)
	if tagValue == "" {
		return &ParsedTag{
			ColumnName: p.namingStrategy.ColumnName(fieldName),
		}, nil
	}

	// Check cache for previously parsed tags (significant performance improvement)
	cacheKey := fieldName + ":" + tagValue
	p.cacheMu.RLock()
	if cached, exists := p.cache[cacheKey]; exists {
		p.cacheMu.RUnlock()
		return cached, nil
	}
	p.cacheMu.RUnlock()

	// Parse tag value and cache result
	parsed, err := p.parseTagValue(fieldName, tagValue)
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", fieldName, err)
	}

	// Cache parsed result for future access
	p.cacheMu.Lock()
	p.cache[cacheKey] = parsed
	p.cacheMu.Unlock()

	return parsed, nil
}

// parseTagValue parses the actual tag value string into structured configuration.
// Handles both simple column names and complex option lists.
//
// Tag format: "option1;option2;key1:value1;key2:value2"
func (p *TagParser) parseTagValue(fieldName, tagValue string) (*ParsedTag, error) {
	// Handle skip directive
	if tagValue == "-" {
		return &ParsedTag{Skip: true}, nil
	}

	// Initialize with default column name
	parsed := &ParsedTag{
		ColumnName: p.namingStrategy.ColumnName(fieldName),
	}

	// Handle simple column name (most common case)
	if !strings.ContainsAny(tagValue, ";:") {
		parsed.ColumnName = tagValue
		return parsed, nil
	}

	// Parse complex options
	options := strings.Split(tagValue, ";")
	for _, option := range options {
		option = strings.TrimSpace(option)
		if option == "" {
			continue
		}

		if err := p.parseOption(parsed, option); err != nil {
			return nil, err
		}
	}

	return parsed, nil
}

// parseOption parses a single tag option (flag or key:value pair).
func (p *TagParser) parseOption(tag *ParsedTag, option string) error {
	// Check for key:value format
	if colonIdx := strings.IndexByte(option, ':'); colonIdx != -1 {
		key := strings.TrimSpace(option[:colonIdx])
		value := strings.TrimSpace(option[colonIdx+1:])
		return p.parseKeyValue(tag, key, value)
	}

	// Handle boolean flags
	return p.parseFlag(tag, option)
}

// parseFlag handles boolean flag options (no values).
func (p *TagParser) parseFlag(tag *ParsedTag, flag string) error {
	switch flag {
	case "primary", "primary_key":
		tag.Primary = true
	case "unique":
		tag.Unique = "" // Simple unique constraint
	case "index":
		tag.Index = "" // Simple index
	case "null":
		tag.Null = true
	case "not_null", "not null":
		tag.NotNull = true
	case "auto_now_add":
		tag.AutoNowAdd = true
	case "auto_now":
		tag.AutoNow = true
	case "auto_generate", "auto":
		tag.AutoGenerate = true
	default:
		// Ignore unknown flags for forward compatibility
	}
	return nil
}

// parseKeyValue handles key:value options with validation.
func (p *TagParser) parseKeyValue(tag *ParsedTag, key, value string) error {
	switch key {
	case "column", "name":
		tag.ColumnName = value

	case "type":
		tag.Type = value // Custom database type (e.g., varchar(255))

	case "default":
		tag.Default = value

	case "unique":
		tag.Unique = value // Named unique constraint

	case "index":
		tag.Index = value // Named index

	case "fk", "foreign_key", "references":
		tag.ForeignKey = value

	case "generator", "gen":
		tag.Generator = value
		tag.AutoGenerate = true

	case "min_length", "min_len":
		return p.parseIntValue(value, &tag.MinLength, "min_length")

	case "max_length", "max_len":
		return p.parseIntValue(value, &tag.MaxLength, "max_length")

	case "enum", "in":
		// Support both pipe and comma separators
		if strings.Contains(value, "|") {
			tag.Enum = strings.Split(value, "|")
		} else {
			tag.Enum = strings.Split(value, ",")
		}
		// Trim whitespace from enum values
		for i, v := range tag.Enum {
			tag.Enum[i] = strings.TrimSpace(v)
		}

	default:
		// Ignore unknown key:value pairs for extensibility
	}

	return nil
}

// parseIntValue parses an integer value and assigns it to the target pointer.
func (p *TagParser) parseIntValue(value string, target **int, fieldName string) error {
	val, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid %s value '%s': must be integer", fieldName, value)
	}
	if val < 0 {
		return fmt.Errorf("invalid %s value %d: must be non-negative", fieldName, val)
	}
	*target = &val
	return nil
}

// ClearCache removes all cached parsed tags.
// Useful for testing or dynamic schema scenarios.
func (p *TagParser) ClearCache() {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	clear(p.cache)
}

// GetCacheSize returns the current number of cached parsed tags.
// Useful for monitoring and performance tuning.
func (p *TagParser) GetCacheSize() int {
	p.cacheMu.RLock()
	defer p.cacheMu.RUnlock()
	return len(p.cache)
}

// Helper methods for ParsedTag
// ============================

// ShouldAutoGenerate returns true if this field should have auto-generated values.
func (tag *ParsedTag) ShouldAutoGenerate() bool {
	return tag.AutoGenerate || tag.Generator != ""
}

// GetGenerator returns the configured ID generator instance.
// Returns nil if no generator is configured or if the generator name is invalid.
func (tag *ParsedTag) GetGenerator() IDGenerator {
	if tag.Generator == "" {
		return nil
	}

	if generator, exists := defaultRegistry.Get(tag.Generator); exists {
		return generator
	}

	return nil
}

// IsSkipped returns true if this field should be skipped entirely.
func (tag *ParsedTag) IsSkipped() bool {
	return tag.Skip
}

// HasValidation returns true if this field has any validation constraints.
func (tag *ParsedTag) HasValidation() bool {
	return tag.MinLength != nil || tag.MaxLength != nil || len(tag.Enum) > 0
}

// IsNullable returns true if this field explicitly allows NULL values.
func (tag *ParsedTag) IsNullable() bool {
	return tag.Null && !tag.NotNull
}

// HasIndex returns true if this field should have a database index.
func (tag *ParsedTag) HasIndex() bool {
	return tag.Index != "" || tag.Primary || tag.Unique != ""
}

// GetConstraintName returns a constraint name for this field, or empty string.
// Useful for generating DDL statements with named constraints.
func (tag *ParsedTag) GetConstraintName(constraintType string) string {
	switch constraintType {
	case "unique":
		return tag.Unique
	case "index":
		return tag.Index
	case "foreign_key":
		return tag.ForeignKey
	default:
		return ""
	}
}
