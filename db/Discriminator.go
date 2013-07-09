package db

import ()

type Discriminator struct {
	Column *Column
	Value  Tokener
}

func NewDiscriminator(column *Column, value Tokener) Discriminator {
	this := Discriminator{}
	this.Column = column
	this.Value = value
	return this
}

func (this Discriminator) Criteria() *Criteria {
	return Matches(this.Column, this.Value)
}
