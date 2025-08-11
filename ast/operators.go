package ast

type Operator string

const (
	OpEqual              = "="
	OpNotEqual           = "!="
	OpNotEqualAlt        = "<>"
	OpLessThan           = "<"
	OpLessThanOrEqual    = "<="
	OpGreaterThan        = ">"
	OpGreaterThanOrEqual = ">="
	OpSpaceship          = "<=>"
)

// Logical Operators
const (
	OpAnd = "AND"
	OpOr  = "OR"
	OpNot = "NOT"
	OpXor = "XOR"
)

// Pattern Matching
const (
	OpLike      = "LIKE"
	OpNotLike   = "NOT LIKE"
	OpILike     = "ILIKE"
	OpNotILike  = "NOT ILIKE"
	OpSimilarTo = "SIMILAR TO"
	OpRegexp    = "REGEXP"
	OpRLike     = "RLIKE"
)

// Set Operations
const (
	OpIn        = "IN"
	OpNotIn     = "NOT IN"
	OpExists    = "EXISTS"
	OpNotExists = "NOT EXISTS"
	OpAny       = "ANY"
	OpSome      = "SOME"
	OpAll       = "ALL"
)

// Null Operations
const (
	OpIsNull     = "IS NULL"
	OpIsNotNull  = "IS NOT NULL"
	OpIsTrue     = "IS TRUE"
	OpIsNotTrue  = "IS NOT TRUE"
	OpIsFalse    = "IS FALSE"
	OpIsNotFalse = "IS NOT FALSE"
	OpIsUnknown  = "IS UNKNOWN"
)

// Range Operations
const (
	OpBetween    = "BETWEEN"
	OpNotBetween = "NOT BETWEEN"
)

// Arithmetic Operators
const (
	OpAdd      = "+"
	OpSubtract = "-"
	OpMultiply = "*"
	OpDivide   = "/"
	OpModulo   = "%"
	OpPower    = "^"
	OpPowerAlt = "**"
)

// Bitwise Operators
const (
	OpBitwiseAnd = "&"
	OpBitwiseOr  = "|"
	OpBitwiseXor = "#"
	OpBitwiseNot = "~"
	OpLeftShift  = "<<"
	OpRightShift = ">>"
)

// String Operations
const (
	OpConcat    = "||"
	OpConcatAlt = "+"
)

// JSON Operators (PostgreSQL)
const (
	OpJsonExtract     = "->"
	OpJsonExtractText = "->>"
	OpJsonPath        = "#>"
	OpJsonPathText    = "#>>"
	OpJsonContains    = "@>"
	OpJsonContainedBy = "<@"
	OpJsonExists      = "?"
	OpJsonExistsAny   = "?|"
	OpJsonExistsAll   = "?&"
)

// Array Operators
const (
	OpArrayContains    = "@>"
	OpArrayContainedBy = "<@"
	OpArrayOverlap     = "&&"
)

// Full-Text Search
const (
	OpMatch    = "MATCH"
	OpAgainst  = "AGAINST"
	OpContains = "CONTAINS"
	OpFreetext = "FREETEXT"
	OpTsMatch  = "@@"
)

// Database-Specific
const (
	OpPrior     = "PRIOR"
	OpConnectBy = "CONNECT BY"
	OpSounds    = "SOUNDS LIKE"
	OpDivInt    = "DIV"
	OpGlob      = "GLOB"
)
