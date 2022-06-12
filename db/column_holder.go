package db

import (
	tk "github.com/quintans/toolkit"
)

// can hold a Column, a Token or any constante
type ColumnHolder struct {
	Token

	column *Column
	hash   int
}

var _ Tokener = &ColumnHolder{}

func NewColumnHolder(column *Column) *ColumnHolder {
	this := new(ColumnHolder)
	this.Operator = TOKEN_COLUMN
	this.Token.Value = column
	this.column = column
	return this
}

func (c *ColumnHolder) GetAlias() string {
	if c.Alias != "" {
		return c.Alias
	} else {
		return c.column.GetAlias()
	}
}

func (c *ColumnHolder) As(alias string) *ColumnHolder {
	c.Alias = alias
	return c
}

func (c *ColumnHolder) For(tableAlias string) *ColumnHolder {
	c.tableAlias = tableAlias
	return c
}

func (c *ColumnHolder) SetTableAlias(tableAlias string) {
	if c.tableAlias == "" {
		c.tableAlias = tableAlias
	}
}

func (c *ColumnHolder) GetColumn() *Column {
	return c.column
}

/*
func (this *ColumnHolder) String() string {
	sb := tk.NewStrBuffer()
	if this.tableAlias != "" {
		sb.Add(this.tableAlias)
	} else {
		sb.Add(this.column.GetTable().GetName())
	}
	sb.Add(".", this.column.GetName())
	if this.Alias != "" {
		sb.Add(" ", this.Alias)
	}
	return sb.String()
}
*/

func (c *ColumnHolder) Clone() interface{} {
	return NewColumnHolder(c.column).As(c.Alias).For(c.tableAlias)
}

func (c *ColumnHolder) Equals(o interface{}) bool {
	return c == o
}

func (c *ColumnHolder) HashCode() int {
	if c.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, c)
		c.hash = result
	}

	return c.hash
}
