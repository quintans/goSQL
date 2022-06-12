package db

import (
	tk "github.com/quintans/toolkit"
	"github.com/quintans/toolkit/ext"
)

var _ tk.Base = &Token{}

//Converts the interface to a token.
//Returns the token
func tokenizeOne(v interface{}) Tokener {
	var token Tokener
	switch t := v.(type) {
	case *Column:
		token = NewColumnHolder(t)
	case Tokener:
		token = t.Clone().(Tokener)
	case *Query:
		token = SubQuery(t)
	default:
		token = Raw(t)
	}
	return token
}

func tokenizeAll(values []interface{}) []Tokener {
	tokens := make([]Tokener, len(values))
	for k, v := range values {
		tokens[k] = tokenizeOne(v)
	}
	return tokens
}

type Tokener interface {
	tk.Clonable

	GetAlias() string
	SetAlias(alias string)
	SetTableAlias(tableAlias string)
	GetTableAlias() string
	SetPseudoTableAlias(tableAlias string)
	GetPseudoTableAlias() string
	IsNil() bool
	GetMembers() []Tokener
	SetMembers(members ...Tokener)
	SetValue(value interface{})
	GetValue() interface{}
	GetOperator() string
	SetOperator(operator string)
}

type Token struct {
	Operator string
	Members  []Tokener
	Value    interface{}
	Alias    string
	hash     int

	tableAlias       string
	pseudoTableAlias string
}

var _ Tokener = &Token{}

func NewToken(operator string, members ...interface{}) *Token {
	this := new(Token)
	this.Operator = operator
	if members != nil {
		this.Members = tokenizeAll(members)
	}
	return this
}

func NewEndToken(operator string, o interface{}) *Token {
	this := new(Token)
	this.Operator = operator
	this.Value = o
	return this
}

func (t *Token) GetPseudoTableAlias() string {
	if t.pseudoTableAlias != "" {
		return t.pseudoTableAlias
	}
	return t.tableAlias
}

func (t *Token) SetPseudoTableAlias(pseudoTableAlias string) {
	t.pseudoTableAlias = pseudoTableAlias
}

func (t *Token) GetOperator() string {
	return t.Operator
}

func (t *Token) SetOperator(operator string) {
	t.Operator = operator
}

func (t *Token) GetAlias() string {
	return t.Alias
}

func (t *Token) SetAlias(alias string) {
	t.Alias = alias
}

// Propagates table alias
func (t *Token) SetTableAlias(tableAlias string) {
	t.tableAlias = tableAlias
	if t.Members != nil {
		for _, tok := range t.Members {
			// it may contains others. ex: param("foo")
			tok.SetTableAlias(tableAlias)
		}
	}
}

func (t *Token) GetTableAlias() string {
	return t.tableAlias
}

func (t *Token) IsNil() bool {
	return t.Members == nil && ext.IsNil(t.Value)
}

func (t *Token) GetMembers() []Tokener {
	return t.Members
}

func (t *Token) SetMembers(members ...Tokener) {
	t.Members = members
}

func (t *Token) SetValue(value interface{}) {
	t.Value = value
}

func (t *Token) GetValue() interface{} {
	return t.Value
}

func (t *Token) String() string {
	var sb tk.StrBuffer
	sb.Add("{operator=", t.Operator, ", ")
	comma := false
	if t.Members != nil {
		sb.Add("members=[")
		for _, o := range t.Members {
			if comma {
				sb.Add("; ")
			}
			sb.Add(o)
			comma = true
		}
	}
	sb.Add(", alias=", t.Alias, "]}")

	return sb.String()
}

func (t *Token) Clone() interface{} {
	token := new(Token)
	token.Operator = t.Operator
	token.Alias = t.Alias

	/*
		// Deep cloning
		if this.Members != nil {
			otherMembers := make([]Tokener, len(this.Members))
			for i, o := range this.Members {
				// it may contains others. ex: param("foo")
				otherMembers[i] = o.Clone().(Tokener)
			}
			token.Members = otherMembers
		} else if this.Value != nil {
			// value
			if t, ok := this.Value.(tk.Clonable); ok {
				token.Value = t.Clone()
			} else {
				token.Value = this.Value
			}
		}
	*/
	if t.Members != nil {
		otherMembers := make([]Tokener, len(t.Members))
		copy(otherMembers, t.Members)
		token.Members = otherMembers
	} else {
		token.Value = t.Value
	}
	return token
}

func (t *Token) Equals(o interface{}) bool {
	switch tp := o.(type) { //type switch
	case *Token:
		if t.Operator == tp.Operator &&
			t.Alias == tp.Alias && t.matchMembers(tp.Members) {
			return true
		}
	}
	return false
}

func (t *Token) matchMembers(m []Tokener) bool {
	if t.Members == nil || m == nil || len(t.Members) != len(m) {
		return false
	}

	for idx, o := range t.Members {
		if !tk.Match(o, m[idx]) {
			return false
		}
	}

	return true
}

func (t *Token) HashCode() int {
	if t.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, t)
		result = tk.HashString(result, t.Operator)
		result = tk.HashString(result, t.Alias)
		result = tk.Hash(result, t.Members)
		t.hash = result
	}

	return t.hash
}

func (t *Token) Greater(value interface{}) *Criteria {
	return Greater(t, value)
}

func (t *Token) GreaterOrMatch(value interface{}) *Criteria {
	return GreaterOrMatch(t, value)
}

func (t *Token) Lesser(value interface{}) *Criteria {
	return Lesser(t, value)
}

func (t *Token) LesserOrMatch(value interface{}) *Criteria {
	return LesserOrMatch(t, value)
}

func (t *Token) Matches(value interface{}) *Criteria {
	return Matches(t, value)
}

func (t *Token) IMatches(value interface{}) *Criteria {
	return IMatches(t, value)
}

func (t *Token) Like(right interface{}) *Criteria {
	return Like(t, right)
}

func (t *Token) ILike(right interface{}) *Criteria {
	return ILike(t, right)
}

func (t *Token) Different(value interface{}) *Criteria {
	return Different(t, value)
}

func (t *Token) IsNull() *Criteria {
	return IsNull(t)
}
