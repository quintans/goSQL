package db

import (
	coll "github.com/quintans/toolkit/collection"

	"database/sql/driver"
	"errors"
	"reflect"
	"time"
)

type AutoKeyStrategy int

const (
	AUTOKEY_NONE AutoKeyStrategy = iota
	AUTOKEY_BEFORE
	AUTOKEY_RETURNING
	AUTOKEY_AFTER
)

type PreInserter interface {
	PreInsert(store IDb) error
}

type PostInserter interface {
	PostInsert(store IDb)
}

type Insert struct {
	DmlCore
	returnId        bool
	autoKeyStrategy AutoKeyStrategy
	HasKeyValue     bool
}

func NewInsert(db IDb, table *Table) *Insert {
	this := new(Insert)
	this.Super(db, table)
	this.vals = coll.NewLinkedHashMap()
	this.returnId = true

	discriminators := table.GetDiscriminators()
	// several discriminators, at maximum one for each column
	for _, discriminator := range discriminators {
		this.Set(discriminator.Column, discriminator.Value)
	}
	return this
}

func (this *Insert) Alias(alias string) *Insert {
	this.alias(alias)
	return this
}

/*
Definies if the auto key should be retrived.
Returning an Id could mean one more query execution.
It returns the Id by default.
*/
func (this *Insert) ReturnId(returnId bool) *Insert {
	this.returnId = returnId
	return this
}

func (this *Insert) Set(col *Column, value interface{}) *Insert {
	this.DmlCore.set(col, value)
	if this.GetTable().GetSingleKeyColumn() != nil && col.IsKey() {
		this.HasKeyValue = (value != nil)
	}
	return this
}

func (this *Insert) Columns(columns ...*Column) *Insert {
	this.cols = columns
	return this
}

func (this *Insert) Values(vals ...interface{}) *Insert {
	/*
		allmost repeating DmlCore because I need to call this.Set
	*/
	if len(this.cols) == 0 {
		panic("Column set is not yet defined!")
	}

	if len(this.cols) != len(vals) {
		panic("The number of defined cols is diferent from the number of passed vals!")
	}

	for k, col := range this.cols {
		this.Set(col, vals[k])
	}

	return this
}

// Loads sets all the columns of the table to matching bean property
//
// param instance: The instance to match
// return this
func (this *Insert) Submit(instance interface{}) (int64, error) {
	var invalid bool
	typ := reflect.TypeOf(instance)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		if typ.Kind() != reflect.Struct {
			invalid = true
		}
	} else {
		invalid = true
	}

	if invalid {
		return 0, errors.New("The argument must be a struct pointer")
	}

	var mappings map[string]*EntityProperty
	if typ == this.lastType {
		mappings = this.lastMappings
	} else {
		mappings = PopulateMapping("", typ)
		this.lastMappings = mappings
		this.lastType = typ
	}

	elem := reflect.ValueOf(instance)
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}

	var version int64 = 1
	for e := this.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if column.IsVersion() {
			this.Set(column, version)
		} else {
			bp := mappings[column.GetAlias()]
			if bp != nil {
				v := bp.Get(elem)
				if v.IsValid() {
					if v.Kind() == reflect.Ptr && v.IsNil() {
						this.Set(column, nil)
					} else {
						v := v.Interface()
						var value interface{}
						var err error
						switch T := v.(type) {
						case driver.Valuer:
							value, err = T.Value()
							if err != nil {
								return 0, err
							}
							this.Set(column, value)
						default:
							value = v
							// if it is a key column its value
							// has to be diferent than the zero value
							// to be included
							if !column.IsKey() ||
								value != reflect.Zero(reflect.TypeOf(value)).Interface() {
								this.Set(column, value)
							} else {
								this.Set(column, nil)
							}
						}
					}
				}
			}
		}
	}

	// pre trigger
	if t, isT := instance.(PreInserter); isT {
		err := t.PreInsert(this.GetDb())
		if err != nil {
			return 0, err
		}
	}

	key, err := this.Execute()
	if err != nil {
		return 0, err
	}

	column := this.table.GetSingleKeyColumn()
	if column != nil {
		bp := mappings[column.GetAlias()]
		bp.Set(elem, reflect.ValueOf(&key))
	}

	column = this.table.GetVersionColumn()
	if column != nil {
		bp := mappings[column.GetAlias()]
		bp.Set(elem, reflect.ValueOf(&version))
	}

	// post trigger
	if t, isT := instance.(PostInserter); isT {
		t.PostInsert(this.GetDb())
	}

	return key, nil
}

func (this *Insert) getCachedSql() *RawSql {
	if this.rawSQL == nil {
		sql := this.db.GetTranslator().GetSqlForInsert(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}
	return this.rawSQL
}

// returns the last inserted id
func (this *Insert) Execute() (int64, error) {
	table := this.GetTable()
	if table.PreInsertTrigger != nil {
		table.PreInsertTrigger(this)
	}

	var err error
	var lastId int64
	var now time.Time
	strategy := this.db.GetTranslator().GetAutoKeyStrategy()
	singleKeyColumn := this.table.GetSingleKeyColumn()
	switch strategy {
	case AUTOKEY_BEFORE:
		if this.returnId && !this.HasKeyValue && singleKeyColumn != nil {
			if lastId, err = this.getAutoNumber(singleKeyColumn); err != nil {
				return 0, err
			}
			this.Set(singleKeyColumn, lastId)
		}
		rsql := this.getCachedSql()
		this.debugSQL(rsql.OriSql, 1)
		now = time.Now()
		_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		this.debugTime(now, 1)
	case AUTOKEY_RETURNING:
		rsql := this.getCachedSql()
		this.debugSQL(rsql.OriSql, 1)
		now = time.Now()
		if this.HasKeyValue || singleKeyColumn == nil {
			_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		} else {
			params := rsql.BuildValues(this.parameters)
			lastId, err = this.dba.InsertReturning(rsql.Sql, params...)
		}
		this.debugTime(now, 1)
	case AUTOKEY_AFTER:
		rsql := this.getCachedSql()
		this.debugSQL(rsql.OriSql, 1)
		now = time.Now()
		_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		if err != nil {
			return 0, err
		}
		this.debugTime(now, 1)
		if this.returnId && !this.HasKeyValue && singleKeyColumn != nil {
			if lastId, err = this.getAutoNumber(singleKeyColumn); err != nil {
				return 0, err
			}
		}
	}

	logger.Debugf("The inserted Id was: %v", lastId)
	return lastId, err
}

func (this *Insert) getAutoNumber(column *Column) (int64, error) {
	sql := this.db.GetTranslator().GetAutoNumberQuery(column)
	if sql == "" {
		return 0, errors.New("Auto Number Query is undefined")
	}
	var id int64
	this.debugSQL(sql, 2)
	now := time.Now()
	_, err := this.dba.QueryRow(sql, []interface{}{}, &id)
	this.debugTime(now, 2)
	if err != nil {
		return 0, err
	}

	return id, nil
}
