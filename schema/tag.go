package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ParsedTag represents a parsed enorm struct tag with essential fields
type ParsedTag struct {
	// Core fields
	ColumnName string
	Skip       bool

	// Database schema
	Type       reflect.Type
	Null       bool
	NotNull    bool
	Default    string
	Primary    bool
	Unique     string // empty for simple unique, name for named constraint
	Index      string // empty for simple index, name for named index
	ForeignKey string

	// Validation (most common ones)
	MinLength *int
	MaxLength *int
	Enum      []string

	// Auto-management
	AutoNowAdd bool
	AutoNow    bool

	// ID generation
	AutoGenerate bool
	Generator    string
}

// TagParser handles parsing of enorm struct tags
type TagParser struct {
	namingStrategy NamingStrategy
	cache          map[string]*ParsedTag
}

// NamingStrategy converts Go field names to column names
type NamingStrategy interface {
	ColumnName(fieldName string) string
}

// SnakeCaseStrategy converts CamelCase to snake_case
type SnakeCaseStrategy struct{}

// ColumnName converts GoFieldName to go_field_name
func (s SnakeCaseStrategy) ColumnName(fieldName string) string {
	return formatName(fieldName)
}

// NewTagParser creates a new tag parser with snake_case naming
func NewTagParser() *TagParser {
	return &TagParser{
		namingStrategy: SnakeCaseStrategy{},
		cache:          make(map[string]*ParsedTag, 64), // Pre-size cache
	}
}

// ParseTag parses an enorm struct tag and returns the configuration
func (p *TagParser) ParseTag(fieldName string, tag reflect.StructTag) (*ParsedTag, error) {
	tagValue := tag.Get("db")
	if tagValue == "" {
		return &ParsedTag{
			ColumnName: p.namingStrategy.ColumnName(fieldName),
		}, nil
	}

	// Check cache to reduce allocations
	cacheKey := fieldName + ":" + tagValue
	if cached, ok := p.cache[cacheKey]; ok {
		return cached, nil
	}

	parsed, err := p.parseTagValue(fieldName, tagValue)
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", fieldName, err)
	}

	p.cache[cacheKey] = parsed
	return parsed, nil
}

// parseTagValue parses the tag value string into a ParsedTag
func (p *TagParser) parseTagValue(fieldName, tagValue string) (*ParsedTag, error) {
	if tagValue == "-" {
		return &ParsedTag{Skip: true}, nil
	}

	parsed := &ParsedTag{
		ColumnName: p.namingStrategy.ColumnName(fieldName),
	}

	// Split once and reuse
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

// Option parsing methods
// =====================

// parseOption parses a single tag option (either flag or key:value)
func (p *TagParser) parseOption(tag *ParsedTag, option string) error {
	if colonIdx := strings.IndexByte(option, ':'); colonIdx != -1 {
		key := option[:colonIdx]
		value := option[colonIdx+1:]
		return p.parseKeyValue(tag, key, value)
	}

	return p.parseFlag(tag, option)
}

// parseFlag handles boolean flag options
func (p *TagParser) parseFlag(tag *ParsedTag, flag string) error {
	switch flag {
	case "primary":
		tag.Primary = true
	case "unique":
		tag.Unique = ""
	case "index":
		tag.Index = ""
	case "null":
		tag.Null = true
	case "not null":
		tag.NotNull = true
	case "auto_now_add":
		tag.AutoNowAdd = true
	case "auto_now":
		tag.AutoNow = true
	case "auto_generate":
		tag.AutoGenerate = true
	default:
		// Silently ignore unknown flags to be flexible
	}
	return nil
}

// parseKeyValue handles key:value options
func (p *TagParser) parseKeyValue(tag *ParsedTag, key, value string) error {
	switch key {
	case "column":
		tag.ColumnName = value
	case "type":
		tag.Type = GetReflectType(value)
	case "default":
		tag.Default = value
	case "unique":
		tag.Unique = value
	case "index":
		tag.Index = value
	case "fk", "foreign_key":
		tag.ForeignKey = value
	case "generator":
		tag.Generator = value
		tag.AutoGenerate = true
	case "min_length":
		return p.parseInt(value, &tag.MinLength, "min_length")
	case "max_length":
		return p.parseInt(value, &tag.MaxLength, "max_length")
	case "enum":
		tag.Enum = strings.Split(value, "|")
	default:
		// Silently ignore unknown key:value pairs
	}
	return nil
}

// Utility methods
// ==============

// parseInt parses an integer value and assigns it to the target pointer
func (p *TagParser) parseInt(value string, target **int, fieldName string) error {
	val, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid %s value: %s", fieldName, value)
	}
	*target = &val
	return nil
}

// Helper methods for ParsedTag
// ============================

// ShouldAutoGenerate returns true if this field should have auto-generated values
func (tag *ParsedTag) ShouldAutoGenerate() bool {
	return tag.AutoGenerate || tag.Generator != ""
}

// GetGenerator returns the generator name, with fallback based on type
func (tag *ParsedTag) GetGenerator() IDGenerator {
	if generator, exists := defaultRegistry.Get(tag.Generator); exists {
		return generator
	}
	return nil
}

// IsSkipped returns true if this field should be skipped entirely
func (tag *ParsedTag) IsSkipped() bool {
	return tag.Skip
}

// HasValidation returns true if this field has any validation rules
func (tag *ParsedTag) HasValidation() bool {
	return tag.MinLength != nil || tag.MaxLength != nil || len(tag.Enum) > 0
}
