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
	//tk.Base
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

func (this *Token) GetPseudoTableAlias() string {
	if this.pseudoTableAlias != "" {
		return this.pseudoTableAlias
	}
	return this.tableAlias
}

func (this *Token) SetPseudoTableAlias(pseudoTableAlias string) {
	this.pseudoTableAlias = pseudoTableAlias
}

func (this *Token) GetOperator() string {
	return this.Operator
}

func (this *Token) SetOperator(operator string) {
	this.Operator = operator
}

func (this *Token) GetAlias() string {
	return this.Alias
}

func (this *Token) SetAlias(alias string) {
	this.Alias = alias
}

// Propagates table alias
func (this *Token) SetTableAlias(tableAlias string) {
	this.tableAlias = tableAlias
	if this.Members != nil {
		for _, o := range this.Members {
			// it may contains others. ex: param("foo")
			if tok, ok := o.(Tokener); ok {
				tok.SetTableAlias(tableAlias)
			}
		}
	}
}

func (this *Token) GetTableAlias() string {
	return this.tableAlias
}

func (this *Token) IsNil() bool {
	return this.Members == nil && ext.IsNil(this.Value)
}

func (this *Token) GetMembers() []Tokener {
	return this.Members
}

func (this *Token) SetMembers(members ...Tokener) {
	this.Members = members
}

func (this *Token) SetValue(value interface{}) {
	this.Value = value
}

func (this *Token) GetValue() interface{} {
	return this.Value
}

func (this *Token) String() string {
	var sb tk.StrBuffer
	sb.Add("{operator=", this.Operator, ", ")
	comma := false
	if this.Members != nil {
		sb.Add("members=[")
		for _, o := range this.Members {
			if comma {
				sb.Add("; ")
			}
			sb.Add(o)
			comma = true
		}
	}
	sb.Add(", alias=", this.Alias, "]}")

	return sb.String()
}

func (this *Token) Clone() interface{} {
	token := new(Token)
	token.Operator = this.Operator
	token.Alias = this.Alias

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
	return token
}

func (this *Token) Equals(o interface{}) bool {
	switch t := o.(type) { //type switch
	case *Token:
		if this.Operator == t.Operator &&
			this.Alias == t.Alias && this.matchMembers(t.Members) {
			return true
		}
	}
	return false
}

func (this *Token) matchMembers(m []Tokener) bool {
	if this.Members == nil || m == nil || len(this.Members) != len(m) {
		return false
	}

	for idx, o := range this.Members {
		if !tk.Match(o, m[idx]) {
			return false
		}
	}

	return true
}

func (this *Token) HashCode() int {
	if this.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, this)
		result = tk.HashString(result, this.Operator)
		result = tk.HashString(result, this.Alias)
		result = tk.Hash(result, this.Members)
		this.hash = result
	}

	return this.hash
}

func (this *Token) Greater(value interface{}) *Criteria {
	return Greater(this, value)
}

func (this *Token) GreaterOrMatch(value interface{}) *Criteria {
	return GreaterOrMatch(this, value)
}

func (this *Token) Lesser(value interface{}) *Criteria {
	return Lesser(this, value)
}

func (this *Token) LesserOrMatch(value interface{}) *Criteria {
	return LesserOrMatch(this, value)
}

func (this *Token) Matches(value interface{}) *Criteria {
	return Matches(this, value)
}

func (this *Token) IMatches(value interface{}) *Criteria {
	return IMatches(this, value)
}

func (this *Token) Like(right interface{}) *Criteria {
	return Like(this, right)
}

func (this *Token) ILike(right interface{}) *Criteria {
	return ILike(this, right)
}

func (this *Token) Different(value interface{}) *Criteria {
	return Different(this, value)
}

func (this *Token) IsNull() *Criteria {
	return IsNull(this)
}
