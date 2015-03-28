package db

/*
Simple CASE Expression
----------------------

The syntax for a simple CASE expression is:

SELECT CASE ("column_name")
  WHEN "value1" THEN "result1"
  WHEN "value2" THEN "result2"
  ...
  [ELSE "resultN"]
  END
FROM "table_name";


Searched CASE Expression
------------------------

The syntax for a searched CASE expression is:

SELECT CASE
  WHEN "condition1" THEN "result1"
  WHEN "condition2" THEN "result2"
  ...
  [ELSE "resultN"]
  END
FROM "table_name";
*/

type SearchedWhen struct {
	parent   *SearchedCase
	criteria *Criteria
	result   interface{}
}

type SearchedCase struct {
	whens []*SearchedWhen
	other interface{}
	alias string
}

func NewSearchedCase() *SearchedCase {
	this := new(SearchedCase)
	this.whens = make([]*SearchedWhen, 0)
	return this
}

func (this *SearchedCase) If(criteria *Criteria) *SearchedWhen {
	when := new(SearchedWhen)
	when.parent = this
	when.criteria = criteria
	this.whens = append(this.whens, when)
	return when
}

func (this *SearchedWhen) Then(value interface{}) *SearchedCase {
	this.result = value
	return this.parent
}

func (this *SearchedCase) Else(value interface{}) *SearchedCase {
	this.other = value
	return this
}

func (this *SearchedCase) End() *Token {
	vals := make([]interface{}, 0)
	for _, v := range this.whens {
		vals = append(vals, NewToken(TOKEN_CASE_WHEN, v.criteria, v.result))
	}
	if this.other != nil {
		vals = append(vals, NewToken(TOKEN_CASE_ELSE, this.other))
	}
	return NewToken(TOKEN_CASE, vals...)
}

type SimpleWhen struct {
	parent     *SimpleCase
	expression interface{}
	result     interface{}
}

type SimpleCase struct {
	expression interface{}
	whens      []*SimpleWhen
	other      interface{}
	alias      string
}

func NewSimpleCase(expression interface{}) *SimpleCase {
	this := new(SimpleCase)
	this.expression = expression
	return this
}

func (this *SimpleCase) When(expression interface{}) *SimpleWhen {
	when := new(SimpleWhen)
	when.parent = this
	when.expression = expression
	this.whens = append(this.whens, when)
	return when
}

func (this *SimpleWhen) Then(value interface{}) *SimpleCase {
	this.result = value
	return this.parent
}

func (this *SimpleCase) Else(value interface{}) *SimpleCase {
	this.other = value
	return this
}

func (this *SimpleCase) End() *Token {
	vals := make([]interface{}, 0)
	if this.expression != nil {
		vals = append(vals, this.expression)
	}
	for _, v := range this.whens {
		vals = append(vals, NewToken(TOKEN_CASE_WHEN, v.expression, v.result))
	}
	if this.other != nil {
		vals = append(vals, NewToken(TOKEN_CASE_ELSE, this.other))
	}
	return NewToken(TOKEN_CASE, vals...)
}
