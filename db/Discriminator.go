package db

import ()

type Discriminator struct {
	Column   *Column
	Value    Tokener
	Criteria *Criteria
}

func NewDiscriminator(column *Column, value Tokener) Discriminator {
	this := Discriminator{}
	this.Column = column
	this.Value = value
	this.Criteria = Matches(this.Column, this.Value)
	return this
}
