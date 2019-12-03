package db

import (
	"reflect"

	"github.com/pkg/errors"

	"github.com/quintans/goSQL/dbx"
	. "github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/db")

var OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed."
var VERSION_SET_MSG = "Unable to set Version data."

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

func (this *Marker) Mark(mark string) {
	if this.changes == nil {
		this.changes = make(map[string]bool)
	}
	this.changes[mark] = true
}

func (this *Marker) Marks() map[string]bool {
	return this.changes
}

func (this *Marker) Unmark() {
	this.changes = nil
}

type IDb interface {
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

func NewDb(connection dbx.IConnection, translator Translator) *Db {
	this := new(Db)
	this.Overrider = this
	this.Connection = connection
	this.Translator = translator
	return this
}

type Db struct {
	Overrider  IDb
	Connection dbx.IConnection
	Translator Translator

	attributes map[string]interface{}
}

func (this *Db) GetTranslator() Translator {
	return this.Translator
}

func (this *Db) GetConnection() dbx.IConnection {
	return this.Connection
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Query(table *Table) *Query {
	return NewQuery(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Insert(table *Table) *Insert {
	return NewInsert(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Delete(table *Table) *Delete {
	return NewDelete(this, table)
}

// the idea is to centralize the query creation so that future customization could be made
func (this *Db) Update(table *Table) *Update {
	return NewUpdate(this, table)
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
	tab, ok := Tables.Get(Str(typ.Name()))
	if !ok {
		// tries to find using also the strcut package.
		// The package correspondes to the database schema
		tab, ok = Tables.Get(Str(typ.PkgPath() + "." + typ.Name()))
		if !ok {
			return nil, nil, errors.Errorf("There is no table mapped to %s", typ.Name())
		}
	}

	return tab.(*Table), typ, nil
}

func (this *Db) Create(instance interface{}) error {
	table, _, err := structName(instance)
	if err != nil {
		return err
	}

	var dml = this.Overrider.Insert(table)

	_, err = dml.Submit(instance)
	return err
}

// struct field with `sql:"omit"` should be ignored if value is zero in an update.
// in a Retrieve, this field with this tag is also ignored
func (this *Db) acceptColumn(table *Table, t reflect.Type, handler func(*Column)) error {
	mappings, err := PopulateMapping("", t, this.GetTranslator())
	if err != nil {
		return err
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

func (this *Db) Retrieve(instance interface{}, keys ...interface{}) (bool, error) {
	table, t, err := structName(instance)
	if err != nil {
		return false, err
	}

	var dml = this.Overrider.Query(table)
	if err := this.acceptColumn(table, t, func(c *Column) {
		dml.Column(c)
	}); err != nil {
		return false, err
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

func (this *Db) buildCriteria(table *Table, example interface{}) ([]*Criteria, error) {
	criterias := make([]*Criteria, 0)

	s := reflect.ValueOf(example)
	t := reflect.TypeOf(example)
	mappings, err := PopulateMapping("", t, this.GetTranslator())
	if err != nil {
		return nil, err
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

func (this *Db) find(instance interface{}, example interface{}) (*Query, error) {
	table, t, err := structName(instance)
	if err != nil {
		return nil, err
	}

	query := this.Overrider.Query(table)
	if err := this.acceptColumn(table, t, func(c *Column) {
		query.Column(c)
	}); err != nil {
		return nil, err
	}

	criterias, err := this.buildCriteria(table, example)
	if err != nil {
		return nil, err
	}
	if len(criterias) > 0 {
		query.Where(criterias...)
	}

	return query, nil
}

func (this *Db) FindFirst(instance interface{}, example interface{}) (bool, error) {
	query, err := this.find(instance, example)
	if err != nil {
		return false, err
	}
	return query.Limit(1).
		SelectTo(instance)
}

func (this *Db) FindAll(instance interface{}, example interface{}) error {
	query, err := this.find(instance, example)
	if err != nil {
		return err
	}
	return query.List(instance)
}

func (this *Db) Modify(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, err
	}

	var dml = this.Overrider.Update(table)

	var key int64
	key, err = dml.Submit(instance)
	return key != 0, err
}

func (this *Db) Remove(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, err
	}

	var dml = this.Overrider.Delete(table)

	var deleted int64
	deleted, err = dml.Submit(instance)
	return deleted != 0, err
}

// removes all that match the criteria defined by the non zero values by the struct.
func (this *Db) RemoveAll(instance interface{}) (int64, error) {
	table, _, err := structName(instance)
	if err != nil {
		return 0, err
	}

	var dml = this.Overrider.Delete(table)
	criterias, err := this.buildCriteria(table, instance)
	if err != nil {
		return 0, err
	}
	if len(criterias) > 0 {
		dml.Where(criterias...)
	}

	var deleted int64
	deleted, err = dml.Execute()
	return deleted, err
}

//Inserts or Updates a record depending on the value of the Version.
//
//If version is nil or zero, an insert is issue, otherwise an update.
//If there is no version column it returns an error.
func (this *Db) Save(instance interface{}) (bool, error) {
	table, _, err := structName(instance)
	if err != nil {
		return false, err
	}

	verColumn := table.GetVersionColumn()
	if verColumn == nil {
		return false, errors.Errorf("The mapped table %s, must have a mapped version column.", table.GetName())
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
		k, err := this.Overrider.Insert(table).Submit(instance)
		return k != 0, err
	} else {
		k, err := this.Overrider.Update(table).Submit(instance)
		return k != 0, err
	}
}

func (this *Db) GetAttribute(key string) (interface{}, bool) {
	if this.attributes == nil {
		return nil, false
	}
	v, ok := this.attributes[key]
	return v, ok
}

func (this *Db) SetAttribute(key string, value interface{}) {
	if this.attributes == nil {
		this.attributes = make(map[string]interface{})
	}
	this.attributes[key] = value
}
