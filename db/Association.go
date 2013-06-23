package db

import (
	"fmt"

	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collection"
)

type ColGroup []*Column

func (this ColGroup) TO(to ...*Column) Relashionships {
	if len(this) != len(to) {
		panic("The number of source columns is different from the number of target columns.")
	}
	relations := make([]Relation, len(this), len(this))
	for k, from := range this {
		relations[k] = NewRelation(from, to[k])
	}
	return relations
}

func (this ColGroup) WITH(to ...*Column) *Association {
	relations := this.TO(to...)
	return NewAssociation(relations...)
}

type Relashionships []Relation

func (this Relashionships) As(alias string) *Association {
	return NewAssociation(this...).As(alias)
}

func ASSOCIATE(from ...*Column) ColGroup {
	// all columns must belong to the same table
	if len(from) > 0 {
		table := from[0].GetTable()
		for _, source := range from {
			if !table.Equals(source.GetTable()) {
				panic("All columns must belong to the same table")
			}
		}
	}

	return from
}

type Association struct {
	tableMany2Many *Table

	FromM2M *Association
	ToM2M   *Association

	tableFrom *Table
	tableTo   *Table
	relations []Relation

	Alias string

	aliasFrom string
	aliasTo   string

	discriminatorTable *Table
	discriminators     []Discriminator
	criterias          []*Criteria

	hash int
}

var _ tk.Base = &Association{}

func NewAssociationCopy(fk *Association) *Association {
	this := new(Association)
	if fk.IsMany2Many() {
		this.defineM2MAssociation(false, fk.Alias, NewAssociationCopy(fk.FromM2M), NewAssociationCopy(fk.ToM2M))
	} else {
		rels := make([]Relation, len(fk.relations))
		for k, v := range fk.relations {
			rels[k] = v // creates new copy
		}
		this.defineAssociation(false, fk.Alias, rels...)
		this.discriminators = fk.discriminators
	}
	return this
}

func (this *Association) GenericPath() string {
	return fmt.Sprintf("%s (%s->%s)", this.Alias, this.tableFrom.String(), this.tableTo.String())
}

func (this *Association) Path() string {
	return fmt.Sprintf("%s (%s.%s->%s.%s)", this.Alias, this.aliasFrom, this.tableFrom.String(), this.aliasTo, this.tableTo.String())
}

func (this *Association) IsMany2Many() bool {
	return this.tableMany2Many != nil
}

// Many To Many
/*
func NewM2MAssociation(name string, fkFrom *Association, fkTo *Association) *Association {
	this := new(Association)
	this.defineM2MAssociation(true, name, fkFrom, fkTo)
	return this
}
*/

// Creates a many to many association by using the relations defined in each association
func NewM2MAssociation(alias string, fkFrom *Association, fkTo *Association) *Association {
	this := new(Association)
	// build relations targeting origin from middle table
	rels := fkFrom.GetRelations()
	relations := make([]Relation, len(rels), len(rels))
	for k, v := range rels {
		// reversed
		relations[k] = NewRelation(v.To.GetColumn(), v.From.GetColumn())
	}
	from := NewAssociation(relations...)

	// build relations targeting destination from middle table
	rels = fkTo.GetRelations()
	relations = make([]Relation, len(rels), len(rels))
	for k, v := range rels {
		relations[k] = NewRelation(v.From.GetColumn(), v.To.GetColumn())
	}
	to := NewAssociation(relations...)

	this.defineM2MAssociation(true, alias, from, to)
	return this
}

func (this *Association) defineM2MAssociation(associate bool, alias string, fkFrom *Association, fkTo *Association) {
	this.Alias = alias

	this.tableMany2Many = fkFrom.tableFrom

	this.FromM2M = fkFrom
	this.tableFrom = this.FromM2M.tableTo
	this.ToM2M = fkTo
	this.tableTo = this.ToM2M.tableTo

	if associate {
		// informs the tables of this association
		this.tableFrom.AddAssociation(this)
	}
}

func (this *Association) GetTableMany2Many() *Table {
	return this.tableMany2Many
}

func NewAssociation(relations ...Relation) *Association {
	this := new(Association)
	this.defineAssociation(false, "", relations...)
	return this
}

func NewAssociationAs(name string, relations ...Relation) *Association {
	this := new(Association)
	this.defineAssociation(true, name, relations...)
	return this
}

func (this *Association) As(alias string) *Association {
	this.Alias = alias
	return this
}

func (this *Association) And(column *Column, value *Token) *Association {
	if this.discriminators == nil {
		this.discriminators = make([]Discriminator, 0)
		this.criterias = make([]*Criteria, 0)
	}

	if this.discriminatorTable != nil && !this.discriminatorTable.Equals(column.GetTable()) {
		panic("Discriminator columns must belong to the same table." +
			column.String() +
			" does not belong to " +
			this.discriminatorTable.String())
	}

	this.discriminatorTable = column.GetTable()
	discriminator := NewDiscriminator(column, value)
	this.discriminators = append(this.discriminators, discriminator)
	this.criterias = append(this.criterias, discriminator.Criteria)
	return this
}

func (this *Association) defineAssociation(add2Table bool, alias string, relations ...Relation) {
	this.Alias = alias

	tableFrom := relations[0].From.GetColumn().GetTable()
	tableTo := relations[0].To.GetColumn().GetTable()
	// check consistency
	for _, relation := range relations {
		if !tableFrom.Equals(relation.From.GetColumn().GetTable()) {
			panic("left side of " + relation.String() + " does not belong to " + tableFrom.String())
		} else if !tableTo.Equals(relation.To.GetColumn().GetTable()) {
			panic("right side of " + relation.String() + " does not belong to " + tableTo.String())
		}
	}
	this.tableFrom = tableFrom
	this.tableTo = tableTo
	this.relations = relations

	if add2Table {
		tableFrom.AddAssociation(this)
	}
}

func (this *Association) GetAliasFrom() string {
	return this.aliasFrom
}

func (this *Association) SetAliasFrom(aliasFrom string) {
	if this.aliasFrom == "" {
		this.aliasFrom = aliasFrom
	}
}

func (this *Association) GetAliasTo() string {
	return this.aliasTo
}

func (this *Association) SetAliasTo(aliasTo string) {
	if this.aliasTo == "" {
		this.aliasTo = aliasTo
	}
}

func (this *Association) GetTableFrom() *Table {
	return this.tableFrom
}

func (this *Association) GetTableTo() *Table {
	return this.tableTo
}

func (this *Association) GetRelations() []Relation {
	return this.relations
}

func (this *Association) GetDiscriminatorTable() *Table {
	return this.discriminatorTable
}

func (this *Association) GetDiscriminators() []Discriminator {
	return this.discriminators
}

func (this *Association) SetDiscriminators(discriminators ...Discriminator) {
	this.discriminators = discriminators

	this.criterias = make([]*Criteria, len(discriminators))
	for k, v := range discriminators {
		this.criterias[k] = v.Criteria
	}
}

func (this *Association) GetCriterias() []*Criteria {
	return this.criterias
}

func (this *Association) GetLink(chain string, from *Table, foreignKeys coll.Collection) *LinkNav {
	// verifica as tabelas
	var table *Table
	if this.tableFrom.Equals(from) {
		table = this.tableTo
	} else {
		table = this.tableFrom
	}

	foreignKeys.Add(this)
	return table.GetLink(chain, foreignKeys)
}

func (this *Association) String() string {
	return this.Path()
}

func (this *Association) Clone() interface{} {
	return NewAssociationCopy(this)
}

func (this *Association) Equals(o interface{}) bool {
	if this == o {
		return true
	}

	return false
}

func (this *Association) HashCode() int {
	if this.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, this)
		this.hash = result
	}

	return this.hash
}
