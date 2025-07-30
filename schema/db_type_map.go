package schema

import (
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Pre-initialize all reflect.Type values to avoid repeated allocations
var (
	// Basic Go types
	stringType       = reflect.TypeOf("")
	boolType         = reflect.TypeOf(false)
	bytesType        = reflect.TypeOf([]byte{})
	interfaceType    = reflect.TypeOf((*interface{})(nil)).Elem()
	ptrInterfaceType = reflect.TypeOf((*interface{})(nil))
	timeType         = reflect.TypeOf(time.Time{})
	durationType     = reflect.TypeOf(time.Duration(0))

	// Integer types
	int8Type   = reflect.TypeOf(int8(0))
	int16Type  = reflect.TypeOf(int16(0))
	int32Type  = reflect.TypeOf(int32(0))
	int64Type  = reflect.TypeOf(int64(0))
	uint8Type  = reflect.TypeOf(uint8(0))
	uint16Type = reflect.TypeOf(uint16(0))
	uint32Type = reflect.TypeOf(uint32(0))
	uint64Type = reflect.TypeOf(uint64(0))

	// Float types
	float32Type = reflect.TypeOf(float32(0))
	float64Type = reflect.TypeOf(float64(0))

	// Array types
	stringArrayType    = reflect.TypeOf([]string{})
	int32ArrayType     = reflect.TypeOf([]int32{})
	boolArrayType      = reflect.TypeOf([]bool{})
	timeArrayType      = reflect.TypeOf([]time.Time{})
	float32ArrayType   = reflect.TypeOf([]float32{})
	float64ArrayType   = reflect.TypeOf([]float64{})
	interfaceArrayType = reflect.TypeOf([]interface{}{})

	// Map types
	stringInterfaceMapType = reflect.TypeOf(map[string]interface{}{})
	stringFloat64MapType   = reflect.TypeOf(map[string]float64{})
	int32Float32MapType    = reflect.TypeOf(map[int32]float32{})
)

var DBTypeMap = map[string]reflect.Type{
	// ===================
	// STANDARD SQL TYPES
	// ===================

	// Character types
	"CHAR":              stringType,
	"VARCHAR":           stringType,
	"TEXT":              stringType,
	"CLOB":              stringType,
	"NCHAR":             stringType,
	"NVARCHAR":          stringType,
	"NTEXT":             stringType,
	"NCLOB":             stringType,
	"CHARACTER":         stringType,
	"CHAR VARYING":      stringType,
	"CHARACTER VARYING": stringType,

	// Numeric types - Integers
	"TINYINT":   int8Type,
	"SMALLINT":  int16Type,
	"MEDIUMINT": int32Type,
	"INT":       int32Type,
	"INTEGER":   int32Type,
	"BIGINT":    int64Type,
	"SERIAL":    int32Type,
	"BIGSERIAL": int64Type,

	// Numeric types - Unsigned integers
	"UNSIGNED TINYINT":   uint8Type,
	"UNSIGNED SMALLINT":  uint16Type,
	"UNSIGNED MEDIUMINT": uint32Type,
	"UNSIGNED INT":       uint32Type,
	"UNSIGNED INTEGER":   uint32Type,
	"UNSIGNED BIGINT":    uint64Type,

	// Numeric types - Floating point
	"REAL":             float32Type,
	"FLOAT":            float64Type,
	"DOUBLE":           float64Type,
	"DOUBLE PRECISION": float64Type,
	"NUMERIC":          stringType, // Use string for precise decimals
	"DECIMAL":          stringType, // Use string for precise decimals
	"DEC":              stringType,
	"FIXED":            stringType,
	"NUMBER":           stringType,

	// Boolean
	"BOOLEAN": boolType,
	"BOOL":    boolType,
	"BIT":     boolType,

	// Date and Time
	"DATE":      timeType,
	"TIME":      timeType,
	"DATETIME":  timeType,
	"TIMESTAMP": timeType,
	"YEAR":      int16Type,
	"INTERVAL":  durationType,

	// Binary types
	"BINARY":     bytesType,
	"VARBINARY":  bytesType,
	"BLOB":       bytesType,
	"TINYBLOB":   bytesType,
	"MEDIUMBLOB": bytesType,
	"LONGBLOB":   bytesType,
	"BYTEA":      bytesType,
	"RAW":        bytesType,
	"LONG RAW":   bytesType,
	"IMAGE":      bytesType,

	// ===================
	// POSTGRESQL TYPES
	// ===================

	// PostgreSQL specific numeric
	"SMALLSERIAL": int16Type,
	"SERIAL2":     int16Type,
	"SERIAL4":     int32Type,
	"SERIAL8":     int64Type,
	"MONEY":       stringType, // Use string for money types

	// PostgreSQL character types
	"NAME":   stringType,
	"BPCHAR": stringType,

	// PostgreSQL date/time
	"TIMESTAMPTZ":                 timeType,
	"TIMESTAMP WITH TIME ZONE":    timeType,
	"TIMESTAMP WITHOUT TIME ZONE": timeType,
	"TIMETZ":                      timeType,
	"TIME WITH TIME ZONE":         timeType,
	"TIME WITHOUT TIME ZONE":      timeType,

	// PostgreSQL JSON
	"JSON":  interfaceType,
	"JSONB": interfaceType,

	// PostgreSQL arrays
	"ARRAY":       interfaceArrayType,
	"INT[]":       int32ArrayType,
	"TEXT[]":      stringArrayType,
	"VARCHAR[]":   stringArrayType,
	"BOOLEAN[]":   boolArrayType,
	"TIMESTAMP[]": timeArrayType,
	"NUMERIC[]":   stringArrayType,

	// PostgreSQL geometric types
	"POINT":   stringType,
	"LINE":    stringType,
	"LSEG":    stringType,
	"BOX":     stringType,
	"PATH":    stringType,
	"POLYGON": stringType,
	"CIRCLE":  stringType,

	// PostgreSQL network types
	"INET":     stringType,
	"CIDR":     stringType,
	"MACADDR":  stringType,
	"MACADDR8": stringType,

	// PostgreSQL UUID
	"UUID": stringType,

	// PostgreSQL XML
	"XML": stringType,

	// PostgreSQL range types
	"INT4RANGE": stringType,
	"INT8RANGE": stringType,
	"NUMRANGE":  stringType,
	"TSRANGE":   stringType,
	"TSTZRANGE": stringType,
	"DATERANGE": stringType,

	// PostgreSQL full-text search
	"TSVECTOR": stringType,
	"TSQUERY":  stringType,

	// PostgreSQL hierarchical
	"LTREE":     stringType,
	"LQUERY":    stringType,
	"LTXTQUERY": stringType,

	// PostgreSQL other
	"OID":      uint32Type,
	"REGPROC":  uint32Type,
	"REGCLASS": uint32Type,
	"REGTYPE":  uint32Type,

	// ===================
	// MYSQL TYPES
	// ===================

	// MySQL specific
	"ENUM":               stringType,
	"SET":                stringArrayType,
	"GEOMETRY":           bytesType,
	"LINESTRING":         stringType,
	"MULTIPOINT":         stringType,
	"MULTILINESTRING":    stringType,
	"MULTIPOLYGON":       stringType,
	"GEOMETRYCOLLECTION": bytesType,

	// ===================
	// SQL SERVER TYPES
	// ===================

	"UNIQUEIDENTIFIER": stringType,
	"ROWVERSION":       bytesType,
	"SMALLMONEY":       stringType,
	"CURSOR":           ptrInterfaceType,
	"SQL_VARIANT":      interfaceType,
	"TABLE":            interfaceType,
	"HIERARCHYID":      bytesType,
	"GEOGRAPHY":        bytesType,

	// ===================
	// ORACLE TYPES
	// ===================

	"VARCHAR2":     stringType,
	"NVARCHAR2":    stringType,
	"LONG":         stringType,
	"ROWID":        stringType,
	"UROWID":       stringType,
	"BFILE":        bytesType,
	"REF":          ptrInterfaceType,
	"XMLTYPE":      stringType,
	"URITYPE":      stringType,
	"HTTPURITYPE":  stringType,
	"DBURITYPE":    stringType,
	"XDBURITYPE":   stringType,
	"SDO_GEOMETRY": bytesType,
	"ANYTYPE":      interfaceType,
	"ANYDATA":      interfaceType,
	"ANYDATASET":   interfaceType,

	// ===================
	// SQLITE TYPES
	// ===================

	"INT2":              int16Type,
	"UNSIGNED BIG INT":  uint64Type,
	"VARYING CHARACTER": stringType,
	"NATIVE CHARACTER":  stringType,

	// ===================
	// VECTOR TYPES (AI/ML)
	// ===================

	// Generic vectors
	"VECTOR":        float32ArrayType,
	"EMBEDDING":     float32ArrayType,
	"FLOAT_VECTOR":  float32ArrayType,
	"DOUBLE_VECTOR": float64ArrayType,
	"INT_VECTOR":    int32ArrayType,
	"BINARY_VECTOR": bytesType,
	"SPARSE_VECTOR": int32Float32MapType,

	// PostgreSQL pgvector extension (common sizes)
	"VECTOR(1536)": float32ArrayType,
	"VECTOR(384)":  float32ArrayType,
	"VECTOR(768)":  float32ArrayType,
	"VECTOR(512)":  float32ArrayType,
	"VECTOR(256)":  float32ArrayType,
	"VECTOR(128)":  float32ArrayType,

	// Vector arrays
	"FLOAT32_ARRAY": float32ArrayType,
	"FLOAT64_ARRAY": float64ArrayType,

	// Milvus vectors
	"FLOAT_VECTOR_128":  float32ArrayType,
	"FLOAT_VECTOR_256":  float32ArrayType,
	"FLOAT_VECTOR_512":  float32ArrayType,
	"FLOAT_VECTOR_768":  float32ArrayType,
	"FLOAT_VECTOR_1024": float32ArrayType,
	"FLOAT_VECTOR_1536": float32ArrayType,
	"BINARY_VECTOR_128": bytesType,
	"BINARY_VECTOR_256": bytesType,
	"BINARY_VECTOR_512": bytesType,

	// Pinecone style
	"DENSE_VECTOR": float32ArrayType,

	// ===================
	// MODERN EXTENSIONS
	// ===================

	// PostGIS
	"RASTER": bytesType,
	"BOX2D":  stringType,
	"BOX3D":  stringType,

	// TimescaleDB
	"TIMESCALEDB_CONTINUOUS_AGGREGATE": interfaceType,

	// MongoDB-style
	"OBJECTID": bytesType,
	"DOCUMENT": interfaceType,

	// Redis-style
	"HASH": stringInterfaceMapType,
	"LIST": interfaceArrayType,
	"ZSET": stringFloat64MapType,

	// ClickHouse types
	"INT8":           int8Type,
	"INT16":          int16Type,
	"INT32":          int32Type,
	"INT64":          int64Type,
	"UINT8":          uint8Type,
	"UINT16":         uint16Type,
	"UINT32":         uint32Type,
	"UINT64":         uint64Type,
	"FLOAT32":        float32Type,
	"FLOAT64":        float64Type,
	"STRING":         stringType,
	"FIXEDSTRING":    stringType,
	"ENUM8":          stringType,
	"ENUM16":         stringType,
	"TUPLE":          interfaceArrayType,
	"NULLABLE":       ptrInterfaceType,
	"LOWCARDINALITY": stringType,

	// ===================
	// GENERIC/FALLBACK
	// ===================
	"UNKNOWN":      interfaceType,
	"USER_DEFINED": interfaceType,
}

// GetReflectType Helper function to get reflect.Type for a database type string
func GetReflectType(dbType string) reflect.Type {
	upperType := strings.ToUpper(dbType)
	if t, exists := DBTypeMap[upperType]; exists {
		return t
	}

	// Handle parameterized types like VARCHAR(255), DECIMAL(10,2), etc.
	if parenIdx := strings.IndexByte(upperType, '('); parenIdx != -1 {
		baseType := upperType[:parenIdx]
		if t, exists := DBTypeMap[baseType]; exists {
			return t
		}
	}

	// Fallback to interface{}
	return interfaceType
}

// IsVectorType Helper function to check if a type supports vectors
func IsVectorType(dbType string) bool {
	upperType := strings.ToUpper(dbType)
	return strings.Contains(upperType, "VECTOR") ||
		strings.Contains(upperType, "EMBEDDING") ||
		strings.HasSuffix(upperType, "_ARRAY")
}

// GetVectorDimension Helper function to get vector dimension from type string
func GetVectorDimension(dbType string) int {
	if parenIdx := strings.IndexByte(dbType, '('); parenIdx != -1 {
		if closeIdx := strings.IndexByte(dbType[parenIdx:], ')'); closeIdx != -1 {
			dimStr := dbType[parenIdx+1 : parenIdx+closeIdx]
			if dim, err := strconv.Atoi(dimStr); err == nil {
				return dim
			}
		}
	}
	return 0 // Unknown dimension
}
