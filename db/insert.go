package db

import (
	"github.com/quintans/faults"
	coll "github.com/quintans/toolkit/collections"

	"database/sql/driver"
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
	returnId    bool
	HasKeyValue bool

	err error
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

func (i *Insert) Alias(alias string) *Insert {
	i.alias(alias)
	return i
}

//Definies if the auto key should be retrieved.
//Returning an Id could mean one more query execution.
//It returns the Id by default.
func (i *Insert) ReturnId(returnId bool) *Insert {
	i.returnId = returnId
	return i
}

func (i *Insert) Set(col *Column, value interface{}) *Insert {
	if i.err != nil {
		return i
	}
	i.DmlCore.set(col, value)
	if i.GetTable().GetSingleKeyColumn() != nil && col.IsKey() {
		i.HasKeyValue = (value != nil)
	}
	return i
}

func (i *Insert) Columns(columns ...*Column) *Insert {
	if i.err != nil {
		return i
	}

	i.cols = columns
	return i
}

func (i *Insert) Values(vals ...interface{}) *Insert {
	if i.err != nil {
		return i
	}
	/*
		allmost repeating DmlCore because I need to call this.Set
	*/
	if len(i.cols) == 0 {
		return &Insert{
			err: faults.New("column set is empty"),
		}
	}

	if len(i.cols) != len(vals) {
		return &Insert{
			err: faults.Errorf("the number of defined columns (%d) is diferent from the number of passed values (%d)", len(i.cols), len(vals)),
		}
	}

	for k, col := range i.cols {
		i.Set(col, vals[k])
	}

	return i
}

// Loads sets all the columns of the table to matching bean property
//
// param instance: The instance to match
// return this
func (i *Insert) Submit(instance interface{}) (int64, error) {
	if i.err != nil {
		return 0, i.err
	}

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
		return 0, faults.New("The argument must be a struct pointer")
	}

	var mappings map[string]*EntityProperty
	if typ == i.lastType {
		mappings = i.lastMappings
	} else {
		var err error
		mappings, err = PopulateMapping("", typ, i.GetDb().GetTranslator())
		if err != nil {
			return 0, err
		}
		i.lastMappings = mappings
		i.lastType = typ
	}

	elem := reflect.ValueOf(instance)
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}

	var marks map[string]bool
	markable, isMarkable := instance.(Markable)
	if isMarkable {
		marks = markable.Marks()
	}
	useMarks := len(marks) > 0

	var version int64 = 1
	for e := i.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if column.IsVersion() {
			i.Set(column, version)
		} else {
			bp := mappings[column.GetAlias()]
			if bp != nil {
				var marked = !useMarks || marks[column.GetAlias()]
				v := bp.Get(elem)
				if v.IsValid() && (!useMarks || marked) {
					if v.Kind() == reflect.Ptr && v.IsNil() {
						value, err := bp.ConvertToDb(nil)
						if err != nil {
							return 0, err
						}
						i.Set(column, value)
					} else {
						val := v.Interface()
						var value interface{}
						var err error
						switch T := val.(type) {
						case driver.Valuer:
							value, err = T.Value()
							if err != nil {
								return 0, err
							}
							value, err = bp.ConvertToDb(value)
							if err != nil {
								return 0, err
							}
							i.Set(column, value)
						default:
							value, err = bp.ConvertToDb(val)
							if err != nil {
								return 0, err
							}
							// if it is a key column its value
							// has to be diferent than the zero value
							// to be included
							if !column.IsKey() ||
								value != reflect.Zero(reflect.TypeOf(value)).Interface() {
								i.Set(column, value)
							} else {
								i.Set(column, nil)
							}
						}
					}
				}
			}
		}
	}

	// pre trigger
	if t, isT := instance.(PreInserter); isT {
		err := t.PreInsert(i.GetDb())
		if err != nil {
			return 0, err
		}
	}

	hadKeyValue := i.HasKeyValue
	key, err := i.Execute()
	if err != nil {
		return 0, err
	}

	if !hadKeyValue {
		column := i.table.GetSingleKeyColumn()
		if column != nil {
			bp := mappings[column.GetAlias()]
			bp.Set(elem, reflect.ValueOf(&key))
		}
	}

	column := i.table.GetVersionColumn()
	if column != nil {
		bp := mappings[column.GetAlias()]
		if bp != nil {
			bp.Set(elem, reflect.ValueOf(&version))
		}
	}

	// post trigger
	if t, isT := instance.(PostInserter); isT {
		t.PostInsert(i.GetDb())
	}

	if isMarkable {
		markable.Unmark()
	}

	return key, nil
}

func (i *Insert) getCachedSql() *RawSql {
	if i.rawSQL == nil {
		sql := i.db.GetTranslator().GetSqlForInsert(i)
		i.rawSQL = ToRawSql(sql, i.db.GetTranslator())
	}
	return i.rawSQL
}

// returns the last inserted id
func (i *Insert) Execute() (int64, error) {
	if i.err != nil {
		return 0, i.err
	}

	table := i.GetTable()
	if table.PreInsertTrigger != nil {
		table.PreInsertTrigger(i)
	}

	var err error
	var lastId int64
	var now time.Time
	strategy := i.db.GetTranslator().GetAutoKeyStrategy()
	singleKeyColumn := i.table.GetSingleKeyColumn()

	var sql string
	var params []interface{}
	switch strategy {
	case AUTOKEY_BEFORE:
		if i.returnId && !i.HasKeyValue && singleKeyColumn != nil {
			if lastId, err = i.getAutoNumber(singleKeyColumn); err != nil {
				return 0, err
			}
			i.Set(singleKeyColumn, lastId)
		}
		sql, params, err = i.prepareSQL()
		if err != nil {
			return 0, err
		}
		now = time.Now()
		_, err = i.dba.Insert(sql, params...)
		i.debugTime(now, 1)
	case AUTOKEY_RETURNING:
		sql, params, err = i.prepareSQL()
		if err != nil {
			return 0, err
		}
		now = time.Now()
		if i.HasKeyValue || singleKeyColumn == nil {
			_, err = i.dba.Insert(sql, params...)
		} else {
			lastId, err = i.dba.InsertReturning(sql, params...)
		}
		i.debugTime(now, 1)
	case AUTOKEY_AFTER:
		sql, params, err = i.prepareSQL()
		if err != nil {
			return 0, err
		}
		now = time.Now()
		_, err = i.dba.Insert(sql, params...)
		if err != nil {
			return 0, err
		}
		i.debugTime(now, 1)
		if i.returnId && !i.HasKeyValue && singleKeyColumn != nil {
			if lastId, err = i.getAutoNumber(singleKeyColumn); err != nil {
				return 0, err
			}
		}
	}

	logger.Debugf("The inserted Id was: %v", lastId)
	return lastId, err
}

func (i *Insert) prepareSQL() (string, []interface{}, error) {
	rsql := i.getCachedSql()
	i.debugSQL(rsql.OriSql, 1)
	params, err := rsql.BuildValues(i.parameters)
	if err != nil {
		return "", nil, err
	}

	return rsql.Sql, params, nil
}

func (i *Insert) getAutoNumber(column *Column) (int64, error) {
	sql := i.db.GetTranslator().GetAutoNumberQuery(column)
	if sql == "" {
		return 0, faults.New("auto Number Query is undefined")
	}
	var id int64
	i.debugSQL(sql, 2)
	now := time.Now()
	_, err := i.dba.QueryRow(sql, []interface{}{}, &id)
	i.debugTime(now, 2)
	if err != nil {
		return 0, err
	}

	return id, nil
}
