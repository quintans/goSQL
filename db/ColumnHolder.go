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

func (this *ColumnHolder) GetAlias() string {
	if this.Alias != "" {
		return this.Alias
	} else {
		return this.column.GetAlias()
	}
}

func (this *ColumnHolder) As(alias string) *ColumnHolder {
	this.Alias = alias
	return this
}

func (this *ColumnHolder) For(tableAlias string) *ColumnHolder {
	this.tableAlias = tableAlias
	return this
}

func (this *ColumnHolder) SetTableAlias(tableAlias string) {
	if this.tableAlias == "" {
		this.tableAlias = tableAlias
	}
}

func (this *ColumnHolder) GetColumn() *Column {
	return this.column
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

func (this *ColumnHolder) Clone() interface{} {
	return NewColumnHolder(this.column).As(this.Alias).For(this.tableAlias)
}

func (this *ColumnHolder) Equals(o interface{}) bool {
	if this == o {
		return true
	}
	return false
}

func (this *ColumnHolder) HashCode() int {
	if this.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, this)
		this.hash = result
	}

	return this.hash
}
