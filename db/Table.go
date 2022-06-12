package db

import (
	"github.com/quintans/faults"
	coll "github.com/quintans/toolkit/collections"
	"github.com/quintans/toolkit/ext"

	"strings"

	"github.com/quintans/goSQL/dbx"
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

	PreInsertTrigger func(*Insert)
	PreUpdateTrigger func(*Update)
	PreDeleteTrigger func(*Delete)

	err error
}

func TABLE(name string) *Table {
	if name == "" {
		return &Table{
			err: faults.New("empty for table name is not allowed"),
		}
	}
	this := new(Table).As(dbx.ToCamelCase(name))
	this.columnsMap = coll.NewLinkedHashMap()
	this.columns = coll.NewLinkedHashSet()
	this.keys = coll.NewLinkedHashSet()
	this.name = name
	AddEntity(this)

	return this
}

func (t *Table) As(alias string) *Table {
	if alias == "" {
		return &Table{
			err: faults.New("empty for table alias is not allowed"),
		}
	}
	t.Alias = alias
	return t
}

// gets the table name
func (t *Table) GetName() string {
	return t.name
}

func (t *Table) COLUMN(name string) *Column {
	if t.err != nil {
		return &Column{
			err: t.err,
		}
	}

	col := new(Column)
	col.name = name
	col.alias = dbx.ToCamelCase(name)

	col.table = t
	if !t.columns.Contains(col) {
		t.columns.Add(col)

		// checks if this column alias uniqueness
		if _, ok := t.columnsMap.Get(ext.Str(col.GetAlias())); ok {
			return &Column{
				err: faults.Errorf("The alias '%s' for the column '%s' is not unique!", col.GetAlias(), col.String()),
			}
		} else {
			t.columnsMap.Put(ext.Str(col.GetAlias()), col)
		}
	}
	return col
}

func (t *Table) KEY(name string) *Column {
	return t.COLUMN(name).Key()
}

func (t *Table) VERSION(name string) *Column {
	return t.COLUMN(name).Version()
}

func (t *Table) DELETION(name string) *Column {
	return t.COLUMN(name).Deletion()
}

func (t *Table) addKey(col *Column) {
	if t.err != nil {
		return
	}

	t.keys.Add(col)
	if t.keys.Size() == 1 {
		t.singleKey = col
	} else {
		// it is only allowed one single key column
		t.singleKey = nil
	}
}

func (t *Table) setVersion(col *Column) {
	t.version = col
}

func (t *Table) setDeletion(col *Column) {
	t.deletion = col
}

func (t *Table) With(column string, value interface{}) *Table {
	if t.err != nil {
		return t
	}

	if t.discriminators == nil {
		t.discriminators = make([]Discriminator, 0)
	}
	token := tokenizeOne(value)
	discriminator := NewDiscriminator(t.COLUMN(column), token)
	t.discriminators = append(t.discriminators, discriminator)
	return t
}

func (t *Table) ASSOCIATE(from ...*Column) ColGroup {
	if t.err != nil {
		return ColGroup{err: t.err}
	}
	// all columns must be from this table.
	for _, source := range from {
		if !t.Equals(source.GetTable()) {
			return ColGroup{
				err: faults.New(source.String() + " does not belong to " + t.String()),
			}
		}
	}

	return ColGroup{
		cols: from,
	}
}

func (t *Table) ASSOCIATION(from *Column, to *Column) *Association {
	if t.err != nil {
		return &Association{err: t.err}
	}

	if !t.Equals(from.GetTable()) {
		return &Association{
			err: faults.New(from.String() + " does not belong to " + t.String()),
		}
	}
	return NewAssociation(NewRelation(from, to))
}

// gets column list
func (t *Table) GetColumns() coll.Collection {
	return t.columns
}

func (t *Table) GetBasicColumns() coll.Collection {
	list := coll.NewArrayList()
	for e := list.Enumerator(); e.HasNext(); {
		if column, ok := e.Next().(*Column); ok && !column.IsKey() && !column.IsVersion() && !column.IsDeletion() {
			list.Add(column)
		}
	}
	return list
}

func (t *Table) String() string {
	return t.name
}

func (t *Table) GetKeyColumns() coll.Collection {
	return t.keys
}

func (t *Table) GetSingleKeyColumn() *Column {
	return t.singleKey
}

func (t *Table) GetVersionColumn() *Column {
	return t.version
}

func (t *Table) GetDeletionColumn() *Column {
	return t.deletion
}

func (t *Table) Equals(obj interface{}) bool {
	if t == obj {
		return true
	}

	switch tp := obj.(type) { //type switch
	case *Table:
		return t.Alias == tp.Alias &&
			strings.EqualFold(t.name, tp.GetName())
	}

	return false
}

func (t *Table) AddAssociation(fk *Association) *Association {
	return t.AddAssociationAs(fk.Alias, fk)
}

func (t *Table) AddAssociationAs(name string, fk *Association) *Association {
	if t.err != nil {
		return &Association{err: t.err}
	}

	key := ext.Str(name)

	if t.associationMap == nil {
		t.associationMap = coll.NewLinkedHashMap()
	} else {
		if value, ok := t.associationMap.Get(key); ok {
			return &Association{
				err: faults.Errorf("An association %s is already mapped to this table (%s) with the key %s", value, t.Alias, name),
			}
		}
	}

	t.associationMap.Put(key, fk)
	return fk
}

func (t *Table) GetAssociations() []*Association {
	if t.associationMap != nil {
		values := t.associationMap.Values()
		associations := make([]*Association, len(values))
		for k, v := range values {
			associations[k], _ = v.(*Association)
		}
		return associations
	}
	return nil
}

func (t *Table) GetDiscriminators() []Discriminator {
	return t.discriminators
}

func (t *Table) GetCriterias() []*Criteria {
	if len(t.discriminators) > 0 {
		criterias := make([]*Criteria, len(t.discriminators))
		for k, v := range t.discriminators {
			criterias[k] = v.Criteria()
		}
		return criterias
	} else {
		return nil
	}
}
