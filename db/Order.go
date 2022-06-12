package db

type Order struct {
	alias  string
	column *ColumnHolder
	asc    bool
}

func NewOrder(column *ColumnHolder) *Order {
	this := new(Order)
	this.column = column
	this.asc = true
	return this
}

func NewOrderAs(alias string) *Order {
	this := new(Order)
	this.alias = alias
	this.asc = true
	return this
}

func (o Order) GetAlias() string {
	return o.alias
}

func (o *Order) GetHolder() *ColumnHolder {
	return o.column
}

func (o *Order) Asc(asc bool) *Order {
	o.asc = asc
	return o
}

func (o *Order) IsAsc() bool {
	return o.asc
}
