package db

import (
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collections"
	"github.com/quintans/toolkit/ext"

	"unicode"
)

// Set of characters that qualify as parameter separators,
// indicating that a parameter name in a SQL String has ended.
const PARAMETER_SEPARATORS = `"':&,;()|=+-*%/\<>^`

//Set of characters that qualify as comment or quotes starting characters.
var START_SKIP = []string{"'", "\"", "--", "/*"}

// Set of characters that at are the corresponding comment or quotes ending characters.
var STOP_SKIP = []string{"'", "\"", "\n", "*/"}

// Parse the SQL statement and locate any placeholders or named parameters.
// Named parameters are substituted for a native placeholder.
//
// param statement: the SQL statement
// return: the parsed statement, represented as ParsedSql instance
func ParseSqlStatement(statement string) *ParsedSql {
	namedParameters := coll.NewHashSet()
	parsedSql := NewParsedSql(statement)

	length := len(statement)
	for i := 0; i < length; i++ {
		c := statement[i]
		if c == ':' || c == '&' {
			j := i + 1
			if j < length && statement[j] == ':' && c == ':' {
				// Postgres-style "::" casting operator - to be skipped.
				i = i + 2
				continue
			}
			for j < length && !isParameterSeparator(rune(statement[j])) {
				j++
			}
			if (j - i) > 1 {
				parameter := ext.Str(statement[i+1 : j])
				if !namedParameters.Contains(parameter) {
					namedParameters.Add(parameter)
				}
				parsedSql.AddNamedParameter(parameter.String(), i, j)
			}
			i = j - 1
		}
	}
	return parsedSql
}

// Determine whether a parameter name ends at the current position,
// that is, whether the given character qualifies as a separator.
func isParameterSeparator(c rune) bool {
	if unicode.IsSpace(c) {
		return true
	}
	for _, ps := range PARAMETER_SEPARATORS {
		if c == ps {
			return true
		}
	}
	return false
}

// Parse the SQL statement and locate any placeholders or named parameters.
// Named parameters are substituted for a '?' placeholder
//
// param parsedSql
//            the parsed represenation of the SQL statement
// param paramSource
//            the source for named parameters
// return the SQL statement with substituted parameters
// see #parseSqlStatement
func SubstituteNamedParameters(parsedSql *ParsedSql, translator Translator) string {
	originalSql := parsedSql.String()
	actualSql := tk.NewStrBuffer()
	paramNames := parsedSql.Names
	lastIndex := 0
	for i, v := range paramNames {
		indexes := parsedSql.Indexes[i]
		startIndex := indexes[0]
		endIndex := indexes[1]
		actualSql.Add(originalSql[lastIndex:startIndex])
		actualSql.Add(translator.GetPlaceholder(i, v))
		lastIndex = endIndex
	}
	actualSql.Add(originalSql[lastIndex:])
	return actualSql.String()
}

// converts SQL with named parameters to the specialized Database placeholders
//
// param sql
//            The SQL to be converted
// param params
//            The named parameters and it's values
// @return The {@link RawSql} with the result
func ToRawSql(sql string, translator Translator) *RawSql {
	rawSql := new(RawSql)
	rawSql.OriSql = sql
	parsedSql := ParseSqlStatement(sql)
	rawSql.Names = parsedSql.Names
	rawSql.Sql = SubstituteNamedParameters(parsedSql, translator)
	return rawSql
}
