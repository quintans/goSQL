package db

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

func (this *Criteria) GetLeft() Tokener {
	if len(this.Members) > 0 {
		return this.Members[0]
	}
	return nil
}

func (this *Criteria) GetRight() Tokener {
	if len(this.Members) > 1 {
		return this.Members[1]
	}
	return nil
}

func (this *Criteria) SetLeft(left interface{}) {
	if len(this.Members) > 0 {
		this.Members[0], _ = tokenizeOne(left)
	}
}

func (this *Criteria) SetRight(right interface{}) {
	if len(this.Members) > 1 {
		this.Members[1], _ = tokenizeOne(right)
	}
}

func (this *Criteria) Clone() interface{} {
	c := NewCriteria(this.Operator)
	c.Token = this.Token.Clone().(*Token)

	if this.IsNot {
		c.Not()
	}
	return c
}

func (this *Criteria) And(criteria *Criteria) *Criteria {
	return And(this, criteria)
}

func (this *Criteria) Or(criteria *Criteria) *Criteria {
	return Or(this, criteria)
}
