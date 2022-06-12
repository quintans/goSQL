package db

/*
 * Criteria
 */
type Criteria struct {
	*Token
	IsNot bool

	err error
}

var _ Tokener = &Criteria{}

func NewCriteria(operator string, members ...interface{}) *Criteria {
	c := new(Criteria)
	c.Token = NewToken(operator, members...)
	return c
}

func (c *Criteria) Not() *Criteria {
	if c.err != nil {
		return c
	}

	c.IsNot = true
	return c
}

func (c *Criteria) GetLeft() Tokener {
	if len(c.Members) > 0 {
		return c.Members[0]
	}
	return nil
}

func (c *Criteria) GetRight() Tokener {
	if len(c.Members) > 1 {
		return c.Members[1]
	}
	return nil
}

func (c *Criteria) SetLeft(left interface{}) {
	if len(c.Members) > 0 {
		c.Members[0] = tokenizeOne(left)
	}
}

func (c *Criteria) SetRight(right interface{}) {
	if len(c.Members) > 1 {
		c.Members[1] = tokenizeOne(right)
	}
}

func (c *Criteria) Clone() interface{} {
	crit := NewCriteria(c.Operator)
	// Deep cloning
	//c.Token = this.Token.Clone().(*Token)
	crit.Token = c.Token

	if c.IsNot {
		crit.Not()
	}
	return crit
}

func (c *Criteria) And(criteria *Criteria) *Criteria {
	if c.err != nil {
		return c
	}
	return And(c, criteria)
}

func (c *Criteria) Or(criteria *Criteria) *Criteria {
	if c.err != nil {
		return c
	}
	return Or(c, criteria)
}
