package db

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/quintans/goSQL/dbx"
	. "github.com/quintans/toolkit/ext"
	"github.com/quintans/toolkit/log"
)

var logger = log.LoggerFor("github.com/quintans/goSQL/db")

func init() {
	// activates output of program file line
	logger.CallDepth(1)
}

var OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed."
var VERSION_SET_MSG = "Unable to set Version data."

type IDb interface {
	GetTranslator() Translator
	GetConnection() dbx.IConnection
	InTransaction() bool

	Query(table *Table) *Query
	Insert(table *Table) *Insert
	Delete(table *Table) *Delete
	Update(table *Table) *Update

	Create(instance interface{}) error
	Retrive(instance interface{}, keys ...interface{}) (bool, error)
	Modify(instance interface{}) (bool, error)
	Remove(instance interface{}) (bool, error)
	Save(instance interface{}) (bool, error) // Create or Modify
}

var _ IDb = &Db{}

func NewDb(inTx bool, connection dbx.IConnection, translator Translator) *Db {
	this := new(Db)
	this.Overrider = this
	this.inTx = inTx
	this.Connection = connection
	this.Translator = translator
	return this
}

type Db struct {
	Overrider  IDb
	inTx       bool
	Connection dbx.IConnection
	Translator Translator

	lastInsert *Insert
	lastUpdate *Update
	lastDelete *Delete
	lastQuery  *Query
}

func (this *Db) InTransaction() bool {
	return this.inTx
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

func (this *Db) getLastInsert(table *Table) *Insert {
	var dml *Insert
	if this.lastInsert != nil && this.lastInsert.GetTable().Equals(table) {
		dml = this.lastInsert
	} else {
		dml = this.Overrider.Insert(table)
		this.lastInsert = dml
	}
	return dml
}

func (this *Db) getLastUpdate(table *Table) *Update {
	var dml *Update
	if this.lastUpdate != nil && this.lastUpdate.GetTable().Equals(table) {
		dml = this.lastUpdate
	} else {
		dml = this.Overrider.Update(table)
		this.lastUpdate = dml
	}
	return dml
}

func structName(instance interface{}) (*Table, error) {
	typ := reflect.TypeOf(instance)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	tab, ok := Tables.Get(Str(typ.Name()))
	if !ok {
		return nil, errors.New("There is no table mapped to " + typ.Name())
	}

	return tab.(*Table), nil
}

func (this *Db) Create(instance interface{}) error {
	table, err := structName(instance)
	if err != nil {
		return err
	}

	// get insert from cache
	var dml = this.getLastInsert(table)
	_, err = dml.Submit(instance)
	return err
}

func (this *Db) Retrive(instance interface{}, keys ...interface{}) (bool, error) {
	table, err := structName(instance)
	if err != nil {
		return false, err
	}

	// get query from cache
	var dml *Query
	if this.lastQuery != nil && this.lastQuery.GetTable().Equals(table) {
		dml = this.lastQuery
	} else {
		dml = this.Overrider.Query(table)
		this.lastQuery = dml
	}

	criterias := make([]*Criteria, 0)
	pos := 0
	for e := table.GetKeyColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if column.IsKey() {
			if len(keys) > pos {
				criterias = append(criterias, column.Matches(keys[pos]))
			}
			pos++
		}
	}

	if len(criterias) > 0 {
		dml.Where(And(criterias...))
	}

	return dml.SelectTo(instance)
}

func (this *Db) Modify(instance interface{}) (bool, error) {
	table, err := structName(instance)
	if err != nil {
		return false, err
	}

	// get Update from cache
	var dml = this.getLastUpdate(table)

	var key int64
	key, err = dml.Submit(instance)
	return key != 0, err
}

func (this *Db) Remove(instance interface{}) (bool, error) {
	table, err := structName(instance)
	if err != nil {
		return false, err
	}

	// get Delete from cache
	var dml *Delete
	if this.lastDelete != nil && this.lastDelete.GetTable().Equals(table) {
		dml = this.lastDelete
	} else {
		dml = this.Overrider.Delete(table)
		this.lastDelete = dml
	}

	var deleted int64
	deleted, err = dml.Submit(instance)
	return deleted != 0, err
}

/*
Inserts or Updates a record depending on the value of the Version value.
If version is nil or zero, an insert is issue, otherwise an update.
If there is no version column it returns an error.
*/
func (this *Db) Save(instance interface{}) (bool, error) {
	table, err := structName(instance)
	if err != nil {
		return false, err
	}

	verColumn := table.GetVersionColumn()
	if verColumn == nil {
		return false, errors.New(fmt.Sprintf("The mapped table %s, does not have a mapped version column.", table.GetName()))
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
		k, err := this.getLastInsert(table).Submit(instance)
		return k != 0, err
	} else {
		k, err := this.getLastUpdate(table).Submit(instance)
		return k != 0, err
	}
}
