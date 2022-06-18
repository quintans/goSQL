package db

import (
	"reflect"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
	"github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/db")

var (
	OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed."
	VERSION_SET_MSG     = "Unable to set Version data."
)

const (
	sqlKey        = "sql"
	sqlOmitionVal = "omit"
	sqlEmbededVal = "embeded"
)

// Interface that a struct must implement to inform what columns where changed
type Markable interface {
	// Retrieve property names that were changed
	//
	// return names of the changed properties
	Marks() map[string]bool
	Unmark()
}

type Marker struct {
	changes map[string]bool
}

var _ Markable = &Marker{}

func (m *Marker) Mark(mark string) {
	if m.changes == nil {
		m.changes = make(map[string]bool)
	}
	m.changes[mark] = true
}

func (m *Marker) Marks() map[string]bool {
	return m.changes
}

func (m *Marker) Unmark() {
	m.changes = nil
}

type TableNamer interface {
	TableName() string
}

type Mapper interface {
	Mappings(typ reflect.Type) (map[string]*StructProperty, error)
}

type IDb interface {
	PopulateMapping(prefix string, typ reflect.Type) (map[string]*EntityProperty, error)
	GetTranslator() Translator
	GetConnection() dbx.IConnection

	Query(table *Table) *Query
	Insert(table *Table) *Insert
	Delete(table *Table) *Delete
	Update(table *Table) *Update

	Create(instance interface{}) error
	Retrieve(instance interface{}, keys ...interface{}) (bool, error)
	FindFirst(instance interface{}, example interface{}) (bool, error)
	FindAll(instance interface{}, example interface{}) error
	Modify(instance interface{}) (bool, error)
	Remove(instance interface{}) (bool, error)
	RemoveAll(instance interface{}) (int64, error)
	Save(instance interface{}) (bool, error) // Create or Modify

	GetAttribute(string) (interface{}, bool)
	SetAttribute(string, interface{}) // general attribute. ex: user in session
}

var _ IDb = &Db{}

func NewDb(connection dbx.IConnection, translator Translator, cacher Mapper) *Db {
	this := new(Db)
	this.Overrider = this
	this.Connection = connection
	this.Translator = translator
	this.mapper = cacher
	return this
}

type Db struct {
	Overrider  IDb
	Connection dbx.IConnection
	Translator Translator

	attributes map[string]interface{}
	mapper     Mapper
}

func (d *Db) GetTranslator() Translator {
	return d.Translator
}

func (d *Db) GetConnection() dbx.IConnection {
	return d.Connection
}

// the idea is to centralize the query creation so that future customization could be made
func (d *Db) Query(table *Table) *Query {
	return NewQuery(d, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (d *Db) Insert(table *Table) *Insert {
	return NewInsert(d, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (d *Db) Delete(table *Table) *Delete {
	return NewDelete(d, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (d *Db) Update(table *Table) *Update {
	return NewUpdate(d, table)
}

// finds the registered table for the passed struct
func structName(instance interface{}) (*Table, reflect.Type, error) {
	typ := reflect.TypeOf(instance)
	// slice
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	} else if typ.Kind() == reflect.Ptr && typ.Elem().Kind() == reflect.Slice {
		typ = typ.Elem().Elem()
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	var tab interface{}
	var ok bool
	if t, isT := instance.(TableNamer); isT {
		tab, ok = Tables.Get(ext.Str(t.TableName()))
		if !ok {
			return nil, nil, faults.Errorf("There is no table mapped to TableName %s", t.TableName())
		}
	} else {
		tab, ok = Tables.Get(ext.Str(typ.Name()))
		if !ok {
			// tries to find using also the struct package.
			// The package corresponds to the database schema
			tab, ok = Tables.Get(ext.Str(typ.PkgPath() + "." + typ.Name()))
			if !ok {
				return nil, nil, faults.Errorf("There is no table mapped to Struct Type %s", typ.Name())
			}
		}
	}

	return tab.(*Table), typ, nil
}

func (d *Db) Create(instance interface{}) error {
	table, _, err := structName(instance)
	if err != nil {
		return faults.Wrap(err)
	}

	dml := d.Overrider.Insert(table)

	_, err = dml.Submit(instance)
	return faults.Wrap(err)
}

// struct field with `sql:"omit"` should be ignored if value is zero in an update.
// in a Retrieve, this field with this tag is also ignored
func (d *Db) acceptColumn(table *Table, t reflect.Type, handler func(*Column)) error {
	mappings, err := d.PopulateMapping("", t)
	if err != nil {
		return faults.Wrap(err)
	}
	cols := table.GetColumns().Elements()
	for _, e := range cols {
		column := e.(*Column)
		bp, ok := mappings[column.GetAlias()]
		if ok {
			if !bp.Omit {
				handler(column)
			}
		}
	}
	return nil
}

func (d *Db) Retrieve(instance interface{}, keys ...interface{}) (bool, error) {
	table, t, err := structName(instance)
	if err != nil {
		return false, faults.Wrap(err)
	}

	dml := d.Overrider.Query(table)
	if err := d.acceptColumn(table, t, func(c *Column) {
		dml.Column(c)
	}); err != nil {
		return false, faults.Wrap(err)
	}

	criterias := make([]*Criteria, 0)
	pos := 0
	for e := table.GetKeyColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if len(keys) > pos {
			criterias = append(criterias, column.Matches(keys[pos]))
		}
		pos++
	}

	if len(criterias) > 0 {
		dml.Where(criterias...)
	}

	return dml.SelectTo(instance)
}

func (d *Db) PopulateMapping(prefix string, typ reflect.Type) (map[string]*EntityProperty, error) {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	attrs, err := d.mapper.Mappings(typ)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	mappings := map[string]*EntityProperty{}

	for k, v := range attrs {
		if prefix != "" {
			k = prefix + k
		}
		mappings[k] = &EntityProperty{
			StructProperty: *v,
		}
	}
	return mappings, nil
}

func isZero(x interface{}) bool {
	if x == nil {
		return true
	} else {
		v := reflect.TypeOf(x)
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Struct {
			return x == reflect.Zero(v).Interface()
		}
	}
	return false
}

func acceptField(omit bool, v interface{}) bool {
	return !omit || !isZero(v)
}

func (d *Db) buildCriteria(table *Table, example interface{}) ([]*Criteria, error) {
	criterias := make([]*Criteria, 0)

	s := reflect.ValueOf(example)
	t := reflect.TypeOf(example)
	mappings, err := d.PopulateMapping("", t)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	cols := table.GetColumns().Elements()
	for _, e := range cols {
		column := e.(*Column)
		bp, ok := mappings[column.GetAlias()]
		if ok {
			v := bp.Get(s).Interface()
			if !isZero(v) {
				criterias = append(criterias, column.Matches(v))
			}
		}
	}
	return criterias, nil
}

func (d *Db) find(instance interface{}, example interface{}) (*Query, error) {
	table, t, err := structName(instance)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	query := d.Overrider.Query(table)
	if err := d.acceptColumn(table, t, func(c *Column) {
		query.Column(c)
	}); err != nil {
		return nil, faults.Wrap(err)
	}

	criterias, err := d.buildCriteria(table, example)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	if len(criterias) > 0 {
		query.Where(criterias...)
	}

	return query, nil
}

func (d *Db) FindFirst(instance interface{}, example interface{}) (bool, error) {
	query, err := d.find(instance, example)
	if err != nil {
		return false, faults.Wrap(err)
	}
	return query.Limit(1).
		SelectTo(instance)
}

func (d *Db) FindAll(instance interface{}, example interface{}) error {
	query, err := d.find(instance, example)
	if err != nil {
		return faults.Wrap(err)
	}
	return query.List(instance)
}

func (d *Db) Modify(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, faults.Wrap(err)
	}

	dml := d.Overrider.Update(table)

	var key int64
	key, err = dml.Submit(instance)
	return key != 0, faults.Wrap(err)
}

func (d *Db) Remove(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, faults.Wrap(err)
	}

	dml := d.Overrider.Delete(table)

	var deleted int64
	deleted, err = dml.Submit(instance)
	return deleted != 0, faults.Wrap(err)
}

// removes all that match the criteria defined by the non zero values by the struct.
func (d *Db) RemoveAll(instance interface{}) (int64, error) {
	table, _, err := structName(instance)
	if err != nil {
		return 0, faults.Wrap(err)
	}

	dml := d.Overrider.Delete(table)
	criterias, err := d.buildCriteria(table, instance)
	if err != nil {
		return 0, faults.Wrap(err)
	}
	if len(criterias) > 0 {
		dml.Where(criterias...)
	}

	var deleted int64
	deleted, err = dml.Execute()
	return deleted, faults.Wrap(err)
}

//Inserts or Updates a record depending on the value of the Version.
//
//If version is nil or zero, an insert is issue, otherwise an update.
//If there is no version column it returns an error.
func (d *Db) Save(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, faults.Wrap(err)
	}

	verColumn := table.GetVersionColumn()
	if verColumn == nil {
		return false, faults.Errorf("The mapped table %s, must have a mapped version column.", table.GetName())
	}

	// find column
	val := reflect.ValueOf(instance)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	v := val.FieldByName(verColumn.GetAlias())
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			var zero int64
			ptr := reflect.New(v.Type()).Elem()
			ptr.Set(reflect.ValueOf(&zero))
			v.Set(ptr)
			v = ptr
		}
		v = v.Elem()
	}

	ver := v.Int()

	if ver == 0 {
		k, err := d.Overrider.Insert(table).Submit(instance)
		return k != 0, faults.Wrap(err)
	} else {
		k, err := d.Overrider.Update(table).Submit(instance)
		return k != 0, faults.Wrap(err)
	}
}

func (d *Db) GetAttribute(key string) (interface{}, bool) {
	if d.attributes == nil {
		return nil, false
	}
	v, ok := d.attributes[key]
	return v, ok
}

func (d *Db) SetAttribute(key string, value interface{}) {
	if d.attributes == nil {
		d.attributes = make(map[string]interface{})
	}
	d.attributes[key] = value
}
