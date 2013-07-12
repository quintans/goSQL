package db

var TOKEN_COLUMN = "COLUMN"

// CONDITIONS
var TOKEN_EQ = "EQ"
var TOKEN_NEQ = "NEQ"
var TOKEN_GT = "GT"
var TOKEN_LT = "LT"
var TOKEN_GTEQ = "GTEQ"
var TOKEN_LTEQ = "LTEQ"
var TOKEN_LIKE = "LIKE"

var TOKEN_IEQ = "IEQ"
var TOKEN_IGT = "IGT"
var TOKEN_ILT = "ILT"
var TOKEN_IGTEQ = "IGTEQ"
var TOKEN_ILTEQ = "ILTEQ"
var TOKEN_ILIKE = "ILIKE"

var TOKEN_IN = "IN"
var TOKEN_RANGE = "RANGE"
var TOKEN_VALUERANGE = "VALUERANGE"
var TOKEN_BOUNDEDRANGE = "BOUNDEDRANGE"
var TOKEN_ISNULL = "ISNULL"
var TOKEN_OR = "OR"
var TOKEN_AND = "AND"

var TOKEN_EXISTS = "EXISTS"
var TOKEN_NOT = "NOT"

// FUNCTIONS
var TOKEN_PARAM = "PARAM" // parameter
var TOKEN_NULL = "NULL"   // sets a predefined value
var TOKEN_RAW = "RAW"     // sets a predefined value
var TOKEN_ASIS = "VAL"    // value is injected to the SQL as is.
var TOKEN_ALIAS = "ALIAS"
var TOKEN_COUNT = "COUNT"               // COUNT(*)
var TOKEN_COUNT_COLUMN = "COUNT_COLUMN" // COUNT(COLUMN)
var TOKEN_SUM = "SUM"
var TOKEN_MAX = "MAX"
var TOKEN_MIN = "MIN"
var TOKEN_RTRIM = "RTRIM"
var TOKEN_UPPER = "UPPER"
var TOKEN_LOWER = "LOWER"

var TOKEN_MULTIPLY = "MULTIPLY"
var TOKEN_DIVIDE = "DIVIDE"
var TOKEN_ADD = "ADD"
var TOKEN_MINUS = "MINUS"

var TOKEN_SUBQUERY = "SUBQUERY"

var TOKEN_COALESCE = "COALESCE"
