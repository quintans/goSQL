package db

import (
	coll "github.com/quintans/toolkit/collection"
	. "github.com/quintans/toolkit/ext"

	"fmt"
	"github.com/quintans/goSQL/dbx"
	"strings"
)

type Table struct {
	columnsMap     coll.Map        // Str -> Column
	associationMap coll.Map        // Str -> Association
	name           string          // table name
	Alias          string          // table alias
	columns        coll.Collection // column set
	keys           coll.Collection // key column set
	singleKey      *Column         // single key
	version        *Column         // column version
	deletion       *Column         // logic deletion column
	discriminators []Discriminator //
	criterias      []*Criteria     //
}

func TABLE(name string) *Table {
	if name == "" {
		panic("Null for table name is not allowed.")
	}
	this := new(Table).As(dbx.ToCamelCase(name))
	this.columnsMap = coll.NewLinkedHashMap()
	this.columns = coll.NewLinkedHashSet()
	this.keys = coll.NewLinkedHashSet()
	this.name = name
	AddEntity(this)

	return this
}

func (this *Table) As(alias string) *Table {
	if alias == "" {
		panic("Null for table alias is not allowed.")
	}
	this.Alias = alias
	return this
}

// gets the table name
func (this *Table) GetName() string {
	return this.name
}

func (this *Table) COLUMN(name string) *Column {
	col := new(Column)
	col.name = name
	col.alias = dbx.ToCamelCase(name)

	col.table = this
	if !this.columns.Contains(col) {
		this.columns.Add(col)

		// checks if this column alias uniqueness
		if _, ok := this.columnsMap.Get(Str(col.GetAlias())); ok {
			panic(fmt.Sprintf("The alias '%s' for the column '%s' is not unique!", col.GetAlias(), col.String()))
		} else {
			this.columnsMap.Put(Str(col.GetAlias()), col)
		}
	}
	return col
}

//	/**
//	 * Constructor that defines a virtual column (references a column in another table)
//	 *
//	 * @Param table
//	 *            The table of this virtual column
//	 * @Param association
//	 *            the association to the table having the real column
//	 * @Param realColumn
//	 *            the real column
//	 */
func (this *Table) VCOLUMN(realColumn *Column, association *Association, discriminators ...Discriminator) *Column {
	col := this.COLUMN(realColumn.name)
	col.As(realColumn.alias)
	col.virtual = newVirtualColumn(realColumn, association, discriminators...)
	return col
}

func (this *Table) KEY(name string) *Column {
	return this.COLUMN(name).Key()
}

func (this *Table) VERSION(name string) *Column {
	return this.COLUMN(name).Version()
}

func (this *Table) addKey(col *Column) {
	this.keys.Add(col)
	if this.keys.Size() == 1 {
		this.singleKey = col
	} else {
		// it is only allowed one single key column
		this.singleKey = nil
	}
}

func (this *Table) setVersion(col *Column) {
	this.version = col
}

func (this *Table) setDeletion(col *Column) {
	this.deletion = col
}

func (this *Table) addDiscriminator(col *Column) {
	if this.discriminators == nil {
		this.discriminators = make([]Discriminator, 0)
		this.criterias = make([]*Criteria, 0)
	}
	discriminator := NewDiscriminator(col, col.GetDiscriminator())
	this.discriminators = append(this.discriminators, discriminator)
	this.criterias = append(this.criterias, discriminator.Criteria)
}

func (this *Table) ASSOCIATE(from ...*Column) ColGroup {
	// all columns must be from this table.
	for _, source := range from {
		if !this.Equals(source.GetTable()) {
			panic(source.String() + " does not belong to " + this.String())
		}
	}

	return from
}

func (this *Table) ASSOCIATION(from *Column, to *Column) *Association {
	if !this.Equals(from.GetTable()) {
		panic(from.String() + " does not belong to " + this.String())
	}
	return NewAssociation(NewRelation(from, to))
}

// gets column list
func (this *Table) GetColumns() coll.Collection {
	return this.columns
}

func (this *Table) GetBasicColumns() coll.Collection {
	list := coll.NewArrayList()
	for e := list.Enumerator(); e.HasNext(); {
		if column, ok := e.Next().(*Column); ok && !column.IsKey() && !column.IsVersion() && !column.IsDeletion() {
			list.Add(column)
		}
	}
	return list
}

func (this *Table) String() string {
	return this.name
}

func (this *Table) GetKeyColumns() coll.Collection {
	return this.keys
}

func (this *Table) GetSingleKeyColumn() *Column {
	return this.singleKey
}

func (this *Table) GetVersionColumn() *Column {
	return this.version
}

func (this *Table) GetDeletionColumn() *Column {
	return this.deletion
}

func (this *Table) Equals(obj interface{}) bool {
	if this == obj {
		return true
	}

	switch t := obj.(type) { //type switch
	case *Table:
		return this.Alias == t.Alias &&
			strings.ToUpper(this.name) == strings.ToUpper(t.GetName())
	}

	return false
}

func (this *Table) AddAssociation(fk *Association) *Association {
	return this.AddAssociationAs(fk.Alias, fk)
}

func (this *Table) AddAssociationAs(name string, fk *Association) *Association {
	key := Str(name)

	if this.associationMap == nil {
		this.associationMap = coll.NewLinkedHashMap()
	} else {
		if value, ok := this.associationMap.Get(key); ok {
			panic(
				fmt.Sprintf("An association %s is already mapped to this table (%s) with the key %s",
					value, this.Alias, name))
		}
	}

	this.associationMap.Put(key, fk)
	return fk
}

func (this *Table) GetAssociations() []*Association {
	if this.associationMap != nil {
		values := this.associationMap.Values()
		associations := make([]*Association, len(values))
		for k, v := range values {
			associations[k], _ = v.(*Association)
		}
		return associations
	}
	return nil
}

func (this *Table) GetDiscriminators() []Discriminator {
	return this.discriminators
}

func (this *Table) GetCriterias() []*Criteria {
	return this.criterias
}

func (this *Table) GetLink(chain string, foreignKeys coll.Collection) *LinkNav {
	idx := strings.Index(chain, FK_NAV_SEP)
	var link Str
	if idx > 0 {
		link = Str(chain[:idx])
	} else {
		link = Str(chain)
	}

	// check columns
	o, _ := this.columnsMap.Get(link)
	c, _ := o.(*Column)

	if c != nil {
		return NewLinkNav(foreignKeys, c)
	}

	var fk *Association
	if this.associationMap != nil {
		o, _ = this.associationMap.Get(link)
		fk, _ = o.(*Association)
	}

	if fk != nil {
		if idx < 0 {
			foreignKeys.Add(fk)
			return NewLinkNav(foreignKeys, c)
		}
		return fk.GetLink(chain[idx+1:], this, foreignKeys)
	}
	return nil
}
