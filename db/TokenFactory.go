package db

import "github.com/quintans/faults"

func Col(column *Column) *ColumnHolder {
	return NewColumnHolder(column)
}

// CRITERIA ===========================

func criteriasToInterface(operations []*Criteria) []interface{} {
	toks := make([]interface{}, len(operations))
	for k, v := range operations {
		toks[k] = v
	}
	return toks
}

func Or(operations ...*Criteria) *Criteria {
	if len(operations) == 1 {
		return operations[0]
	}

	return NewCriteria(TOKEN_OR, criteriasToInterface(operations)...)
}

func And(operations ...*Criteria) *Criteria {
	if len(operations) == 1 {
		return operations[0]
	}

	return NewCriteria(TOKEN_AND, criteriasToInterface(operations)...)
}

func Greater(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_GT, left, right)
}

func GreaterOrMatch(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_GTEQ, left, right)
}

func Lesser(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_LT, left, right)
}

func LesserOrMatch(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_LTEQ, left, right)
}

func Matches(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_EQ, left, right)
}

func Range(receiver, bottom, top interface{}) *Criteria {
	if bottom != nil && top != nil {
		return NewCriteria(TOKEN_RANGE, receiver, bottom, top)
	} else if bottom != nil {
		return GreaterOrMatch(receiver, bottom)
	} else if top != nil {
		return LesserOrMatch(receiver, top)
	}

	return &Criteria{
		err: faults.New("invalid range tokenization"),
	}
}

func ValueRange(bottom, top interface{}, value Tokener) *Criteria {
	return NewCriteria(TOKEN_VALUERANGE, bottom, top, value)
}

func BoundedValueRange(bottom, top interface{}, value Tokener) *Criteria {
	return NewCriteria(TOKEN_BOUNDEDRANGE, bottom, top, value)
}

func IsNull(token interface{}) *Criteria {
	return NewCriteria(TOKEN_ISNULL, token, nil)
}

func In(column interface{}, values ...interface{}) *Criteria {
	var vals []interface{}
	vals = append(vals, column)
	vals = append(vals, values...)
	return NewCriteria(TOKEN_IN, vals...)
}

func IMatches(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_IEQ, left, right)
}

func Like(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_LIKE, left, right)
}

func ILike(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_ILIKE, left, right)
}

func Different(left, right interface{}) *Criteria {
	return NewCriteria(TOKEN_NEQ, left, right)
}

func Exists(token interface{}) *Criteria {
	return NewCriteria(TOKEN_EXISTS, token)
}

func Not(token interface{}) *Criteria {
	return NewCriteria(TOKEN_NOT, token)
}

// FUNCTION =======================
func Param(str string) *Token {
	return NewEndToken(TOKEN_PARAM, str) // RAW info
}

func Null() *Token {
	return NewEndToken(TOKEN_NULL, nil)
}

func Raw(o interface{}) *Token {
	return NewEndToken(TOKEN_RAW, o) // RAW info
}

func AsIs(o interface{}) *Token {
	return NewEndToken(TOKEN_ASIS, o) // AS IS info
}

func Alias(s string) *Token {
	return NewEndToken(TOKEN_ALIAS, s)
}

func Sum(token interface{}) *Token {
	return NewToken(TOKEN_SUM, token)
}

func Max(token interface{}) *Token {
	return NewToken(TOKEN_MAX, token)
}

func Min(token interface{}) *Token {
	return NewToken(TOKEN_MIN, token)
}

func Upper(token interface{}) *Token {
	return NewToken(TOKEN_UPPER, token)
}

func Lower(token interface{}) *Token {
	return NewToken(TOKEN_LOWER, token)
}

// pass nil to ignore column
func Count(column interface{}) *Token {
	if column == nil {
		return NewEndToken(TOKEN_COUNT, nil)
	}
	return NewToken(TOKEN_COUNT_COLUMN, column)
}

func Rtrim(token interface{}) *Token {
	return NewToken(TOKEN_RTRIM, token)
}

// the args can be Columns, Tokens, nil or primitives
func Add(values ...interface{}) *Token {
	return NewToken(TOKEN_ADD, values...)
}

// the args can be Columns, Tokens, nil or primitives
func Minus(values ...interface{}) *Token {
	return NewToken(TOKEN_MINUS, values...)
}

// the args can be Columns, Tokens, nil or primitives
func Multiply(values ...interface{}) *Token {
	return NewToken(TOKEN_MULTIPLY, values...)
}

func SubQuery(sq *Query) *Token {
	return NewEndToken(TOKEN_SUBQUERY, sq)
}

/*
	func Tokener autoNumber(DbNUM o) {
		return NewToken(TOKEN_AUTONUM, NewColumnHolder(o));
	}
*/

func Coalesce(values ...interface{}) *Token {
	return NewToken(TOKEN_COALESCE, values...)
}

func If(criteria *Criteria) *SearchedWhen {
	return NewSearchedCase().If(criteria)
}

func Case(expression interface{}) *SimpleCase {
	return NewSimpleCase(expression)
}
