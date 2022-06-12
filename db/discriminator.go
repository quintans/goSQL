package db

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

func (d Discriminator) Criteria() *Criteria {
	return Matches(d.Column, d.Value)
}
