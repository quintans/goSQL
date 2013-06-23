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

func (this Order) GetAlias() string {
	return this.alias
}

func (this *Order) GetHolder() *ColumnHolder {
	return this.column
}

func (this *Order) Asc(asc bool) *Order {
	this.asc = asc
	return this
}

func (this *Order) IsAsc() bool {
	return this.asc
}
