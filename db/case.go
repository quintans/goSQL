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
}

func NewSearchedCase() *SearchedCase {
	this := new(SearchedCase)
	this.whens = make([]*SearchedWhen, 0)
	return this
}

func (s *SearchedCase) If(criteria *Criteria) *SearchedWhen {
	when := new(SearchedWhen)
	when.parent = s
	when.criteria = criteria
	s.whens = append(s.whens, when)
	return when
}

func (s *SearchedWhen) Then(value interface{}) *SearchedCase {
	s.result = value
	return s.parent
}

func (s *SearchedCase) Else(value interface{}) *SearchedCase {
	s.other = value
	return s
}

func (s *SearchedCase) End() *Token {
	vals := make([]interface{}, 0)
	for _, v := range s.whens {
		vals = append(vals, NewToken(TOKEN_CASE_WHEN, v.criteria, v.result))
	}
	if s.other != nil {
		vals = append(vals, NewToken(TOKEN_CASE_ELSE, s.other))
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
}

func NewSimpleCase(expression interface{}) *SimpleCase {
	this := new(SimpleCase)
	this.expression = expression
	this.whens = make([]*SimpleWhen, 0)
	return this
}

func (s *SimpleCase) When(expression interface{}) *SimpleWhen {
	when := new(SimpleWhen)
	when.parent = s
	when.expression = expression
	s.whens = append(s.whens, when)
	return when
}

func (s *SimpleWhen) Then(value interface{}) *SimpleCase {
	s.result = value
	return s.parent
}

func (s *SimpleCase) Else(value interface{}) *SimpleCase {
	s.other = value
	return s
}

func (s *SimpleCase) End() *Token {
	vals := make([]interface{}, 0)
	if s.expression != nil {
		vals = append(vals, s.expression)
	}
	for _, v := range s.whens {
		vals = append(vals, NewToken(TOKEN_CASE_WHEN, v.expression, v.result))
	}
	if s.other != nil {
		vals = append(vals, NewToken(TOKEN_CASE_ELSE, s.other))
	}
	return NewToken(TOKEN_CASE, vals...)
}
