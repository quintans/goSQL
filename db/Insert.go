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
		this.Set(discriminator.Column, discriminator.Column.GetDiscriminator().Clone())
	}
	return this
}

func (this *Insert) Alias(alias string) *Insert {
	this.alias(alias)
	return this
}

func (this *Insert) ReturnId(returnId bool) *Insert {
	this.returnId = returnId
	return this
}

func (this *Insert) Set(col *Column, value interface{}) *Insert {
	this.DmlCore.set(col, value)
	if col.GetTable().GetSingleKeyColumn() != nil && col.IsKey() {
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
	var mappings map[string]*EntityProperty
	typ := reflect.TypeOf(instance)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ == this.lastType {
		mappings = this.lastMappings
	} else {
		mappings = PopulateMapping("", typ)
		this.lastMappings = mappings
		this.lastType = typ
	}

	var elem reflect.Value
	var calcElem bool = true
	for e := this.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if !column.IsVirtual() {
			if column.IsVersion() {
				this.Set(column, 1)
			} else {
				bp := mappings[column.GetAlias()]
				if bp != nil {
					if calcElem {
						elem = reflect.ValueOf(instance)
						if elem.Kind() == reflect.Ptr {
							elem = elem.Elem()
						}
						calcElem = false
					}
					v := bp.Get(elem)
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

	return this.Execute()
}

func (this *Insert) GetCachedSql() *RawSql {
	if this.rawSQL == nil {
		sql := this.db.GetTranslator().GetSqlForInsert(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}
	return this.rawSQL
}

// returns the last inserted id
func (this *Insert) Execute() (int64, error) {
	var err error
	var lastId int64
	var now time.Time
	strategy := this.db.GetTranslator().GetAutoKeyStrategy()
	switch strategy {
	case AUTOKEY_BEFORE:
		if this.returnId && !this.HasKeyValue {
			column := this.table.GetSingleKeyColumn()
			if lastId, err = this.getAutoNumber(column); err != nil {
				return 0, err
			}
			this.Set(column, lastId)
		}
		rsql := this.GetCachedSql()
		this.debugSQL(rsql.OriSql)
		now = time.Now()
		_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		this.debugTime(now)
	case AUTOKEY_RETURNING:
		rsql := this.GetCachedSql()
		this.debugSQL(rsql.OriSql)
		now = time.Now()
		if this.HasKeyValue {
			_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		} else {
			params := rsql.BuildValues(this.parameters)
			lastId, err = this.dba.InsertReturning(rsql.Sql, params...)
		}
		this.debugTime(now)
	case AUTOKEY_AFTER:
		rsql := this.GetCachedSql()
		this.debugSQL(rsql.OriSql)
		now = time.Now()
		_, err = this.dba.Insert(rsql.Sql, rsql.BuildValues(this.parameters)...)
		this.debugTime(now)
		if this.returnId && !this.HasKeyValue {
			if lastId, err = this.getAutoNumber(this.table.GetSingleKeyColumn()); err != nil {
				return 0, err
			}
		}
	}

	return lastId, err
}

func (this *Insert) getAutoNumber(column *Column) (int64, error) {
	sql := this.db.GetTranslator().GetAutoNumberQuery(column)
	if sql == "" {
		return 0, errors.New("Auto Number Query is undefined")
	}
	var id int64
	this.debugSQL(sql)
	now := time.Now()
	_, err := this.dba.QueryRow(sql, []interface{}{}, &id)
	this.debugTime(now)
	if err != nil {
		return 0, err
	}

	return id, nil
}
