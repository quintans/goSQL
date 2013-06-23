package db

import ()

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

func Greater(left interface{}, right interface{}) *Criteria {
	return NewCriteria(TOKEN_GT, left, right)
}

func GreaterOrMatch(left interface{}, right interface{}) *Criteria {
	return NewCriteria(TOKEN_GTEQ, left, right)
}

func Lesser(left interface{}, right interface{}) *Criteria {
	return NewCriteria(TOKEN_LT, left, right)
}

func LesserOrMatch(left interface{}, right interface{}) *Criteria {
	return NewCriteria(TOKEN_LTEQ, left, right)
}

func Matches(left interface{}, right interface{}) *Criteria {
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

	panic("Invalid Range Tokenization")
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
func Param(str string) Tokener {
	return NewEndToken(TOKEN_PARAM, str) // RAW info
}

func Raw(o interface{}) Tokener {
	return NewEndToken(TOKEN_RAW, o) // RAW info
}

func AsIs(o interface{}) Tokener {
	return NewEndToken(TOKEN_ASIS, o) // AS IS info
}

func As(s string) Tokener {
	return NewEndToken(TOKEN_ALIAS, s)
}

func Sum(token interface{}) Tokener {
	return NewToken(TOKEN_SUM, token)
}

func Max(token interface{}) Tokener {
	return NewToken(TOKEN_MAX, token)
}

func Min(token interface{}) Tokener {
	return NewToken(TOKEN_MIN, token)
}

func Upper(token interface{}) Tokener {
	return NewToken(TOKEN_UPPER, token)
}

func Lower(token interface{}) Tokener {
	return NewToken(TOKEN_LOWER, token)
}

// pass nil to ignore column
func Count(column *Column) Tokener {
	if column == nil {
		return NewEndToken(TOKEN_COUNT, nil)
	}
	return NewToken(TOKEN_COUNT_COLUMN, NewColumnHolder(column))
}

func Rtrim(token interface{}) Tokener {
	return NewToken(TOKEN_RTRIM, token)
}

// the args can be Columns, Tokens, nil or primitives
func Add(values ...interface{}) Tokener {
	return NewToken(TOKEN_ADD, values...)
}

// the args can be Columns, Tokens, nil or primitives
func Minus(values ...interface{}) Tokener {
	return NewToken(TOKEN_MINUS, values...)
}

// the args can be Columns, Tokens, nil or primitives
func Multiply(values ...interface{}) Tokener {
	return NewToken(TOKEN_MULTIPLY, values...)
}

// the args can be Columns, Tokens, nil or primitives
func SecondsDiff(values ...interface{}) Tokener {
	return NewToken(TOKEN_SECONDSDIFF, values...)
}

func SubQuery(sq *Query) Tokener {
	return NewEndToken(TOKEN_SUBQUERY, sq)
}

/*
	func Tokener autoNumber(DbNUM o) {
		return NewToken(TOKEN_AUTONUM, NewColumnHolder(o));
	}
*/
