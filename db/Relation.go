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

func (r Relation) String() string {
	return r.From.String() + " -> " + r.To.String()
}
