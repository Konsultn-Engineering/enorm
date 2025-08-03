package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// type_system.go - Unified type system for Go <-> Database type mapping and high-performance setters
//
// This package provides:
// - Bidirectional type conversion between Go types and database types
// - High-performance unsafe pointer setters for database scanning
// - Multi-database dialect support (PostgreSQL, MySQL, SQLServer, Oracle, SQLite, ClickHouse)
// - Automatic type registration with custom resolvers
// - Vector/embedding type support for AI/ML applications
// - Nullable type detection and handling
//

// =========================================================================
// Type Definitions and Constants
// =========================================================================

// Pre-initialized reflect.Type constants to avoid repeated allocations
var (
	// Basic Go types
	stringType    = reflect.TypeOf("")
	boolType      = reflect.TypeOf(false)
	bytesType     = reflect.TypeOf([]byte{})
	timeType      = reflect.TypeOf(time.Time{})
	durationType  = reflect.TypeOf(time.Duration(0))
	interfaceType = reflect.TypeOf((*interface{})(nil)).Elem()

	// Numeric types
	intType     = reflect.TypeOf(int(0))
	int8Type    = reflect.TypeOf(int8(0))
	int16Type   = reflect.TypeOf(int16(0))
	int32Type   = reflect.TypeOf(int32(0))
	int64Type   = reflect.TypeOf(int64(0))
	uintType    = reflect.TypeOf(uint(0))
	uint8Type   = reflect.TypeOf(uint8(0))
	uint16Type  = reflect.TypeOf(uint16(0))
	uint32Type  = reflect.TypeOf(uint32(0))
	uint64Type  = reflect.TypeOf(uint64(0))
	float32Type = reflect.TypeOf(float32(0))
	float64Type = reflect.TypeOf(float64(0))

	// Collection types
	stringArrayType    = reflect.TypeOf([]string{})
	int32ArrayType     = reflect.TypeOf([]int32{})
	boolArrayType      = reflect.TypeOf([]bool{})
	timeArrayType      = reflect.TypeOf([]time.Time{})
	float32ArrayType   = reflect.TypeOf([]float32{})
	float64ArrayType   = reflect.TypeOf([]float64{})
	interfaceArrayType = reflect.TypeOf([]interface{}{})

	// Map types for JSON-like data
	stringInterfaceMapType = reflect.TypeOf(map[string]interface{}{})
	stringFloat64MapType   = reflect.TypeOf(map[string]float64{})
	int32Float32MapType    = reflect.TypeOf(map[int32]float32{})
)

// DatabaseDialect represents supported database systems
type DatabaseDialect string

const (
	DialectPostgreSQL DatabaseDialect = "postgresql"
	DialectMySQL      DatabaseDialect = "mysql"
	DialectSQLServer  DatabaseDialect = "sqlserver"
	DialectOracle     DatabaseDialect = "oracle"
	DialectSQLite     DatabaseDialect = "sqlite"
	DialectClickHouse DatabaseDialect = "clickhouse"
	DialectGeneric    DatabaseDialect = "generic"
)

// =========================================================================
// Database Type Mapping System
// =========================================================================

// dbTypeMap maps database type strings to Go reflect.Type
// Supports major SQL databases plus modern extensions (vectors, JSON, etc.)
var dbTypeMap = map[string]reflect.Type{
	// Standard SQL types
	"VARCHAR": stringType, "TEXT": stringType, "CHAR": stringType,
	"INTEGER": int32Type, "BIGINT": int64Type, "SMALLINT": int16Type, "TINYINT": int8Type,
	"REAL": float32Type, "DOUBLE": float64Type, "FLOAT": float64Type,
	"BOOLEAN": boolType, "BOOL": boolType,
	"TIMESTAMP": timeType, "DATETIME": timeType, "DATE": timeType,
	"BYTEA": bytesType, "BLOB": bytesType, "BINARY": bytesType,

	// PostgreSQL extensions
	"JSONB": interfaceType, "JSON": interfaceType, "UUID": stringType,
	"VECTOR": float32ArrayType, "TEXT[]": stringArrayType, "INT[]": int32ArrayType,
	"TIMESTAMPTZ": timeType, "INET": stringType, "CIDR": stringType,

	// MySQL specific
	"LONGTEXT": stringType, "MEDIUMTEXT": stringType, "LONGBLOB": bytesType,
	"ENUM": stringType, "SET": stringArrayType,

	// SQL Server
	"NVARCHAR": stringType, "UNIQUEIDENTIFIER": stringType, "DATETIME2": timeType,
	"VARBINARY": bytesType, "BIT": boolType,

	// Vector/AI types
	"EMBEDDING": float32ArrayType, "DENSE_VECTOR": float32ArrayType,
	"FLOAT_VECTOR": float32ArrayType, "BINARY_VECTOR": bytesType,

	// Fallback
	"UNKNOWN": interfaceType,
}

// goToDBTypeMap provides reverse lookup from Go types to database types
var goToDBTypeMap = map[reflect.Type]string{
	stringType:             "VARCHAR",
	boolType:               "BOOLEAN",
	bytesType:              "BYTEA",
	timeType:               "TIMESTAMP",
	int32Type:              "INTEGER",
	int64Type:              "BIGINT",
	float32Type:            "REAL",
	float64Type:            "DOUBLE PRECISION",
	float32ArrayType:       "VECTOR",
	stringArrayType:        "TEXT[]",
	interfaceType:          "JSONB",
	stringInterfaceMapType: "JSONB",
}

// dialectSpecificTypes maps Go types to database-specific type names
var dialectSpecificTypes = map[DatabaseDialect]map[reflect.Type]string{
	DialectPostgreSQL: {
		stringType: "VARCHAR", boolType: "BOOLEAN", bytesType: "BYTEA",
		timeType: "TIMESTAMP", float32ArrayType: "VECTOR", interfaceType: "JSONB",
	},
	DialectMySQL: {
		stringType: "VARCHAR(255)", boolType: "TINYINT(1)", bytesType: "LONGBLOB",
		timeType: "DATETIME", float32ArrayType: "JSON", interfaceType: "JSON",
	},
	DialectSQLServer: {
		stringType: "NVARCHAR(MAX)", boolType: "BIT", bytesType: "VARBINARY(MAX)",
		timeType: "DATETIME2", float32ArrayType: "NVARCHAR(MAX)", interfaceType: "NVARCHAR(MAX)",
	},
}

// =========================================================================
// Type Conversion Functions
// =========================================================================

// GetReflectType converts database type string to Go reflect.Type
func GetReflectType(dbType string) reflect.Type {
	upperType := strings.ToUpper(dbType)
	if t, exists := dbTypeMap[upperType]; exists {
		return t
	}

	// Handle parameterized types like VARCHAR(255)
	if parenIdx := strings.IndexByte(upperType, '('); parenIdx != -1 {
		baseType := upperType[:parenIdx]
		if t, exists := dbTypeMap[baseType]; exists {
			return t
		}
	}

	return interfaceType // Safe fallback
}

// GetDatabaseType converts Go reflect.Type to database type string
func GetDatabaseType(goType reflect.Type) string {
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	if dbType, exists := goToDBTypeMap[goType]; exists {
		return dbType
	}

	// Handle slices as arrays
	if goType.Kind() == reflect.Slice {
		elemType := goType.Elem()
		if elemType == float32Type || elemType == float64Type {
			return "VECTOR" // Assume vector for float arrays
		}
		return GetDatabaseType(elemType) + "[]"
	}

	// Handle by kind
	switch goType.Kind() {
	case reflect.String:
		return "VARCHAR"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.Struct:
		if goType == timeType {
			return "TIMESTAMP"
		}
		return "JSONB"
	case reflect.Map, reflect.Interface:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// GetDatabaseTypeForDialect gets database type for specific dialect
func GetDatabaseTypeForDialect(goType reflect.Type, dialect DatabaseDialect) string {
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	if dialectMap, exists := dialectSpecificTypes[dialect]; exists {
		if dbType, exists := dialectMap[goType]; exists {
			return dbType
		}
	}

	return GetDatabaseType(goType)
}

// IsVectorType checks if a type represents vector data
func IsVectorType(dbType string) bool {
	upperType := strings.ToUpper(dbType)
	return strings.Contains(upperType, "VECTOR") ||
		strings.Contains(upperType, "EMBEDDING") ||
		strings.HasSuffix(upperType, "_ARRAY")
}

// IsVectorGoType checks if a Go type represents vector data
func IsVectorGoType(goType reflect.Type) bool {
	if goType.Kind() == reflect.Ptr {
		goType = goType.Elem()
	}

	if goType.Kind() == reflect.Slice {
		elemType := goType.Elem()
		switch elemType.Kind() {
		case reflect.Float32, reflect.Float64, reflect.Int32:
			return true
		default:
		}
	}

	return goType == float32ArrayType || goType == float64ArrayType
}

// GetVectorDimension extracts dimension from vector type string like VECTOR(1536)
func GetVectorDimension(dbType string) int {
	if parenIdx := strings.IndexByte(dbType, '('); parenIdx != -1 {
		if closeIdx := strings.IndexByte(dbType[parenIdx:], ')'); closeIdx != -1 {
			dimStr := dbType[parenIdx+1 : parenIdx+closeIdx]
			if dim, err := strconv.Atoi(dimStr); err == nil {
				return dim
			}
		}
	}
	return 0
}

// =========================================================================
// High-Performance Setter System
// =========================================================================

// TypeResolver provides custom type resolution and database mapping
type TypeResolver interface {
	ResolveType(userType reflect.Type) reflect.Type
	PreProcess(value any) (any, error)
	GetDatabaseType(goType reflect.Type, dialect DatabaseDialect) string
}

// DefaultTypeResolver provides standard type resolution
type DefaultTypeResolver struct{}

func (r DefaultTypeResolver) ResolveType(userType reflect.Type) reflect.Type {
	for userType.Kind() == reflect.Ptr {
		userType = userType.Elem()
	}
	return userType
}

func (r DefaultTypeResolver) PreProcess(value any) (any, error) {
	return value, nil
}

func (r DefaultTypeResolver) GetDatabaseType(goType reflect.Type, dialect DatabaseDialect) string {
	return GetDatabaseTypeForDialect(goType, dialect)
}

// RegisteredTypeInfo holds metadata about registered types
type RegisteredTypeInfo struct {
	GoType       reflect.Type
	DatabaseType string
	Resolver     TypeResolver
	IsVector     bool
	IsNullable   bool
}

// Global registries
var (
	typeResolvers   = sync.Map{} // map[reflect.Type]TypeResolver
	registeredTypes = sync.Map{} // map[reflect.Type]*RegisteredTypeInfo
)

// RegisterSetterCreator registers a type with high-performance setter and database mapping
func RegisterSetterCreator[T any](resolver ...TypeResolver) {
	var zero T
	userType := reflect.TypeOf(zero)

	var typeResolver TypeResolver = DefaultTypeResolver{}
	if len(resolver) > 0 {
		typeResolver = resolver[0]
	}

	typeResolvers.Store(userType, typeResolver)

	// Store type metadata
	typeInfo := &RegisteredTypeInfo{
		GoType:       userType,
		DatabaseType: typeResolver.GetDatabaseType(userType, DialectPostgreSQL),
		Resolver:     typeResolver,
		IsVector:     IsVectorGoType(userType),
		IsNullable:   isNullableType(userType),
	}
	registeredTypes.Store(userType, typeInfo)

	// Create optimized setter with converter cache
	commonConverters := make(map[reflect.Type]func(any) (T, error), 8)

	setterCreators.Store(userType, func(offset uintptr) func(unsafe.Pointer, any) {
		return func(structPtr unsafe.Pointer, value any) {
			fieldPtr := (*T)(unsafe.Add(structPtr, offset))

			if value == nil {
				*fieldPtr = zero
				return
			}

			// Apply preprocessing
			processedValue, err := typeResolver.PreProcess(value)
			if err != nil {
				panic(fmt.Sprintf("preprocessing failed for %s: %v", userType, err))
			}

			actualValue := processedValue
			var actualValueType reflect.Type

			if val := reflect.ValueOf(processedValue); val.Kind() == reflect.Ptr && !val.IsNil() {
				actualValue = val.Elem().Interface()
				actualValueType = val.Type().Elem()
			} else {
				actualValueType = reflect.TypeOf(processedValue)
			}

			// Check cached converters
			if cachedConverter, exists := commonConverters[actualValueType]; exists {
				if converted, err := cachedConverter(actualValue); err == nil {
					*fieldPtr = converted
					return
				}
			}

			// Direct assignment
			if reflect.TypeOf(actualValue) == userType {
				if converted, ok := actualValue.(T); ok {
					*fieldPtr = converted
					return
				}
			}

			// Use converter system
			converter, err := GetConverter(zero, actualValueType)
			if err != nil {
				panic(fmt.Sprintf("no converter for %T -> %T: %v", actualValue, zero, err))
			}

			converted, err := converter(actualValue)
			if err != nil {
				panic(fmt.Sprintf("conversion failed for %s: %v", userType, err))
			}

			// Cache successful converter
			if len(commonConverters) < 16 {
				commonConverters[actualValueType] = func(v any) (T, error) {
					result, err := converter(v)
					if err != nil {
						return zero, err
					}
					return result, nil
				}
			}

			*fieldPtr = converted
		}
	})
}

// =========================================================================
// Specialized Type Resolvers
// =========================================================================

// VectorTypeResolver handles vector types with dimension information
type VectorTypeResolver[T any] struct {
	dimension int
}

func (r *VectorTypeResolver[T]) ResolveType(userType reflect.Type) reflect.Type {
	return userType
}

func (r *VectorTypeResolver[T]) PreProcess(value any) (any, error) {
	return value, nil
}

func (r *VectorTypeResolver[T]) GetDatabaseType(goType reflect.Type, dialect DatabaseDialect) string {
	if r.dimension > 0 && dialect == DialectPostgreSQL {
		return fmt.Sprintf("VECTOR(%d)", r.dimension)
	}
	return GetDatabaseTypeForDialect(goType, dialect)
}

// TypeAliasResolver handles simple type aliases using reflection for safe conversion
type TypeAliasResolver[T, U any] struct{}

func (r TypeAliasResolver[T, U]) ResolveType(userType reflect.Type) reflect.Type {
	var zero U
	return reflect.TypeOf(zero)
}

func (r TypeAliasResolver[T, U]) PreProcess(value any) (any, error) {
	if value == nil {
		return value, nil
	}

	// Try direct conversion first
	if converted, ok := value.(U); ok {
		// Use reflection to safely convert between T and U
		uValue := reflect.ValueOf(converted)
		var tType reflect.Type
		var zero T
		tType = reflect.TypeOf(zero)

		// Check if types are convertible
		if uValue.Type().ConvertibleTo(tType) {
			convertedValue := uValue.Convert(tType)
			return convertedValue.Interface(), nil
		}
	}

	return value, nil
}

func (r TypeAliasResolver[T, U]) GetDatabaseType(goType reflect.Type, dialect DatabaseDialect) string {
	var zero U
	return GetDatabaseTypeForDialect(reflect.TypeOf(zero), dialect)
}

// =========================================================================
// Registration Functions
// =========================================================================

// RegisterBasicType registers a basic Go type
func RegisterBasicType[T any]() {
	RegisterSetterCreator[T]()
}

// RegisterVectorType registers a vector type with dimension
func RegisterVectorType[T any](dimension int) {
	resolver := &VectorTypeResolver[T]{dimension: dimension}
	RegisterSetterCreator[T](resolver)
}

// RegisterTypeAlias registers a type alias
func RegisterTypeAlias[T, U any]() {
	resolver := &TypeAliasResolver[T, U]{}
	RegisterSetterCreator[T](resolver)
}

// RegisterCustomType registers a type with custom resolver
func RegisterCustomType[T any](resolver TypeResolver) {
	RegisterSetterCreator[T](resolver)
}

// =========================================================================
// Utility Functions
// =========================================================================

// isNullableType detects nullable types
func isNullableType(goType reflect.Type) bool {
	if goType.Kind() == reflect.Ptr {
		return true
	}
	typeName := goType.String()
	return strings.HasPrefix(typeName, "sql.Null") ||
		strings.Contains(typeName, "Nullable") ||
		strings.Contains(typeName, "Optional")
}

// GetRegisteredTypes returns all registered type information
func GetRegisteredTypes() map[reflect.Type]*RegisteredTypeInfo {
	result := make(map[reflect.Type]*RegisteredTypeInfo)
	registeredTypes.Range(func(key, value any) bool {
		result[key.(reflect.Type)] = value.(*RegisteredTypeInfo)
		return true
	})
	return result
}

// GetTypeInfo returns information about a registered type
func GetTypeInfo(goType reflect.Type) (*RegisteredTypeInfo, bool) {
	if info, ok := registeredTypes.Load(goType); ok {
		return info.(*RegisteredTypeInfo), true
	}
	return nil, false
}

// IsRegisteredType checks if a type has been registered
func IsRegisteredType(goType reflect.Type) bool {
	_, exists := registeredTypes.Load(goType)
	return exists
}
