package schema

import (
	"fmt"
	"reflect"
)

// buildMeta constructs comprehensive metadata for a struct type, including
// all field information, setter functions, and lookup maps needed for
// high-performance database operations.
//
// This function performs expensive reflection operations once and caches
// the results to avoid repeated computation during database scanning.
//
// Parameters:
//   - t: reflect.Type of the struct to analyze (pointer types are dereferenced)
//
// Returns: Complete EntityMeta with all pre-compiled optimizations
// buildMeta constructs comprehensive metadata for a struct type.
// Performs expensive reflection operations once and caches results.
//
// This is a simplified, cleaner version that focuses on core functionality
// without the confusing multiple setter variants.
func buildMeta(t reflect.Type) (*EntityMeta, error) {
	// Normalize pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid model type: expected struct, got %s", t.Kind())
	}

	// Count exported fields for proper map sizing
	numFields := t.NumField()
	exportedCount := 0
	for i := 0; i < numFields; i++ {
		if t.Field(i).IsExported() && !t.Field(i).Anonymous {
			exportedCount++
		}
	}

	// Initialize metadata with exact capacity
	meta := &EntityMeta{
		Type:         t,
		Name:         t.Name(),
		Fields:       make([]*FieldMeta, 0, exportedCount),
		FieldMap:     make(map[string]*FieldMeta, exportedCount),
		ColumnMap:    make(map[string]*FieldMeta, exportedCount),
		AliasMapping: make(map[string]string, exportedCount),
	}

	// Determine table name
	if tn, ok := reflect.New(t).Interface().(TableNamer); ok {
		meta.TableName = tn.TableName() // Ensure consistency
	} else {
		meta.TableName = schemaContext.namingStrategy.TableName(t.Name()) // Use normalize function
	}

	// Initialize tag parser
	parser := NewTagParser(schemaContext.namingStrategy)

	// Process each field
	for i := 0; i < numFields; i++ {
		f := t.Field(i)

		// Skip unexported/anonymous fields
		if !f.IsExported() || f.Anonymous {
			continue
		}

		// Parse tags
		parsedTag, err := parser.ParseTag(f.Name, f.Tag)
		if err != nil {
			return nil, fmt.Errorf("error parsing tag for field %s: %w", f.Name, err)
		}

		// Skip fields marked with db:"-"
		if parsedTag.IsSkipped() {
			continue
		}

		// Create field metadata
		fm := &FieldMeta{
			Name:       f.Name,
			DBName:     parsedTag.ColumnName,
			Type:       f.Type,
			DBType:     parsedTag.Type,
			Index:      f.Index,
			Tag:        parsedTag,
			IsExported: true,
			Offset:     f.Offset,
			Generator:  parsedTag.GetGenerator(),
		}

		// Set default column name if not specified
		if fm.DBName == "" {
			fm.DBName = schemaContext.namingStrategy.ColumnName(parsedTag.ColumnName) // Ensure consistency
		}

		// Create optimized setter (ONLY ONE TYPE - no confusion)
		fm.DirectSet = createDirectSetterForType(f.Type, f.Offset)

		// Build lookup maps
		meta.Fields = append(meta.Fields, fm)
		meta.FieldMap[f.Name] = fm
		meta.ColumnMap[fm.DBName] = fm
		meta.AliasMapping[fm.DBName] = f.Name
	}

	// Check for custom scanner
	if fn := getRegisteredScanner(t); fn != nil {
		meta.ScannerFn = fn
	}

	return meta, nil
}
