package db

type Relation struct {
	From *ColumnHolder
	To   *ColumnHolder
}

func NewRelation(from *Column, to *Column) Relation {
	this := Relation{}
	this.From = NewColumnHolder(from)
	this.To = NewColumnHolder(to)
	return this
}

func (this Relation) String() string {
	return this.From.String() + " -> " + this.To.String()
}
