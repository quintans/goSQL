package db

import (
	"fmt"

	"github.com/quintans/faults"
	tk "github.com/quintans/toolkit"
)

type ColGroup struct {
	cols []*Column
	err  error
}

func (cg ColGroup) TO(to ...*Column) Relashionships {
	if cg.err != nil {
		return Relashionships{}
	}

	if len(cg.cols) != len(to) {
		return Relashionships{
			err: faults.Errorf("the number of source columns (%d) is different from the number of target columns (%d).", len(cg.cols), len(to)),
		}
	}
	relations := make([]Relation, len(cg.cols))
	for k, from := range cg.cols {
		relations[k] = NewRelation(from, to[k])
	}
	return Relashionships{
		rels: relations,
	}
}

func (cg ColGroup) WITH(to ...*Column) *Association {
	if cg.err != nil {
		return &Association{
			err: cg.err,
		}
	}

	r := cg.TO(to...)
	if r.err != nil {
		return &Association{}
	}
	return NewAssociation(r.rels...)
}

type Relashionships struct {
	rels []Relation
	err  error
}

func (r Relashionships) Fault() error {
	return r.err
}

func (r Relashionships) As(alias string) *Association {
	if r.err != nil {
		return &Association{err: r.err}
	}

	return NewAssociation(r.rels...).As(alias)
}

func ASSOCIATE(from ...*Column) ColGroup {
	// all columns must belong to the same table
	if len(from) > 0 {
		table := from[0].GetTable()
		for _, source := range from {
			if !table.Equals(source.GetTable()) {
				return ColGroup{
					err: faults.Errorf("column '%s' must belong to table '%s'", source, table),
				}
			}
		}
	}
	return ColGroup{
		cols: from,
	}
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

	hash int

	err error
}

var _ tk.Base = &Association{}
var _ tk.Clonable = &Association{}

func NewAssociationCopy(fk *Association) *Association {
	this := new(Association)
	if fk.IsMany2Many() {
		this.defineM2MAssociation(false, fk.Alias, NewAssociationCopy(fk.FromM2M), NewAssociationCopy(fk.ToM2M))
	} else {
		rels := make([]Relation, len(fk.relations))
		for k, v := range fk.relations {
			rels[k] = NewRelation(v.From.GetColumn(), v.To.GetColumn())
		}
		if err := this.defineAssociation(false, fk.Alias, rels...); err != nil {
			return &Association{
				err: err,
			}
		}
		this.discriminators = fk.discriminators
	}
	return this
}

func (a *Association) GenericPath() string {
	if a.err != nil {
		return a.err.Error()
	}
	return fmt.Sprintf("%s (%s->%s)", a.Alias, a.tableFrom.String(), a.tableTo.String())
}

func (a *Association) Path() string {
	if a.err != nil {
		return a.err.Error()
	}
	return fmt.Sprintf("%s (%s.%s->%s.%s)", a.Alias, a.aliasFrom, a.tableFrom.String(), a.aliasTo, a.tableTo.String())
}

func (a *Association) IsMany2Many() bool {
	if a.err != nil {
		return false
	}
	return a.tableMany2Many != nil
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
	this.defineM2MAssociation(true, alias, fkFrom, fkTo)
	return this
}

func (a *Association) defineM2MAssociation(associate bool, alias string, fkFrom *Association, fkTo *Association) {
	a.Alias = alias

	a.tableMany2Many = fkFrom.tableTo

	a.FromM2M = fkFrom
	a.tableFrom = a.FromM2M.tableFrom
	a.ToM2M = fkTo
	a.tableTo = a.ToM2M.tableTo

	if associate {
		// informs the tables of this association
		a.tableFrom.AddAssociation(a)
	}
}

func (a *Association) GetTableMany2Many() *Table {
	return a.tableMany2Many
}

func NewAssociation(relations ...Relation) *Association {
	a := new(Association)
	if err := a.defineAssociation(false, "", relations...); err != nil {
		return &Association{
			err: err,
		}
	}
	return a
}

func NewAssociationAs(name string, relations ...Relation) *Association {
	a := new(Association)
	if err := a.defineAssociation(true, name, relations...); err != nil {
		return &Association{
			err: err,
		}
	}
	return a
}

func (a *Association) As(alias string) *Association {
	if a.err != nil {
		return a
	}
	a.Alias = alias
	return a
}

func (a *Association) With(column *Column, value interface{}) *Association {
	if a.err != nil {
		return a
	}
	if a.discriminators == nil {
		a.discriminators = make([]Discriminator, 0)
	}

	if a.discriminatorTable != nil && !a.discriminatorTable.Equals(column.GetTable()) {
		return &Association{
			err: faults.New("discriminator columns must belong to the same table." +
				column.String() +
				" does not belong to " +
				a.discriminatorTable.String()),
		}
	}

	a.discriminatorTable = column.GetTable()
	token := tokenizeOne(value)
	discriminator := NewDiscriminator(column, token)
	a.discriminators = append(a.discriminators, discriminator)
	return a
}

func (a *Association) defineAssociation(add2Table bool, alias string, relations ...Relation) error {
	if a.err != nil {
		return a.err
	}
	a.Alias = alias

	tableFrom := relations[0].From.GetColumn().GetTable()
	tableTo := relations[0].To.GetColumn().GetTable()
	// check consistency
	for _, relation := range relations {
		if !tableFrom.Equals(relation.From.GetColumn().GetTable()) {
			return faults.Errorf("left side of '%s' does not belong to '%s'", relation, tableFrom)
		} else if !tableTo.Equals(relation.To.GetColumn().GetTable()) {
			return faults.Errorf("right side of '%s' does not belong to '%s'", relation, tableTo)
		}
	}
	a.tableFrom = tableFrom
	a.tableTo = tableTo
	a.relations = relations

	if add2Table {
		tableFrom.AddAssociation(a)
	}
	return nil
}

func (a *Association) GetAliasFrom() string {
	return a.aliasFrom
}

func (a *Association) SetAliasFrom(aliasFrom string) {
	if a.err != nil {
		return
	}
	if a.aliasFrom == "" {
		a.aliasFrom = aliasFrom
	}
}

func (a *Association) GetAliasTo() string {
	return a.aliasTo
}

func (a *Association) SetAliasTo(aliasTo string) {
	if a.err != nil {
		return
	}
	if a.aliasTo == "" {
		a.aliasTo = aliasTo
	}
}

func (a *Association) GetTableFrom() *Table {
	return a.tableFrom
}

func (a *Association) GetTableTo() *Table {
	return a.tableTo
}

func (a *Association) GetRelations() []Relation {
	return a.relations
}

func (a *Association) GetDiscriminatorTable() *Table {
	return a.discriminatorTable
}

func (a *Association) GetDiscriminators() []Discriminator {
	return a.discriminators
}

func (a *Association) SetDiscriminators(discriminators ...Discriminator) {
	a.discriminators = discriminators
}

func (a *Association) String() string {
	return a.Path()
}

func (a *Association) Clone() interface{} {
	return NewAssociationCopy(a)
}

func (a *Association) Equals(o interface{}) bool {
	return a == o
}

func (a *Association) HashCode() int {
	if a.hash == 0 {
		result := tk.HashType(tk.HASH_SEED, a)
		a.hash = result
	}

	return a.hash
}
