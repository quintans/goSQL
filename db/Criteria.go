package db

import (
	tk "github.com/quintans/toolkit"
)

/*
 * Criteria
 */
type Criteria struct {
	*Token
	IsNot bool
}

var _ Tokener = &Criteria{}

func NewCriteria(operator string, members ...interface{}) *Criteria {
	c := new(Criteria)
	c.Token = NewToken(operator, members...)
	return c
}

func (this *Criteria) Not() *Criteria {
	this.IsNot = true
	return this
}

func (this *Criteria) GetLeft() tk.Base {
	if this.Members != nil && len(this.Members) > 0 {
		return this.Members[0].(tk.Base)
	}
	return nil
}

func (this *Criteria) GetRight() tk.Base {
	if this.Members != nil && len(this.Members) > 1 {
		return this.Members[1].(tk.Base)
	}
	return nil
}

func (this *Criteria) Clone() interface{} {
	c := NewCriteria(this.Operator, nil)
	c.Token = this.Token.Clone().(*Token)

	if this.IsNot {
		c.Not()
	}
	return c
}
