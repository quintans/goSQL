package db

import (
	"github.com/quintans/goSQL/dbx"
	coll "github.com/quintans/toolkit/collection"

	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

type Update struct {
	DmlCore
}

func NewUpdate(db IDb, table *Table) *Update {
	this := new(Update)
	this.Super(db, table)
	this.vals = coll.NewLinkedHashMap()
	return this
}

func (this *Update) Alias(alias string) *Update {
	this.alias(alias)
	return this
}

func (this *Update) Set(col *Column, value interface{}) *Update {
	this.DmlCore.set(col, value)
	return this
}

func (this *Update) Columns(columns ...*Column) *Update {
	this.cols = columns
	return this
}

func (this *Update) Values(vals ...interface{}) *Update {
	this.DmlCore.values(vals...)
	return this
}

//  Loads sets all the columns of the table to matching bean property
//
// param <T>
// param bean
//             The bean to match
// return affected rows
func (this *Update) Submit(value interface{}) (int64, error) {
	var mappings map[string]*EntityProperty
	var criterias []*Criteria

	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ == this.lastType {
		mappings = this.lastMappings
	} else {
		mappings = PopulateMapping("", typ)
		criterias = make([]*Criteria, 0)
		this.criteria = nil
		this.lastMappings = mappings
		this.lastType = typ
	}

	var mustSucceed bool
	var id interface{}
	var ver int64
	for e := this.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if !column.IsVirtual() {
			alias := column.GetAlias()
			bp := mappings[alias]
			if bp != nil {
				//val := bp.Get(reflect.ValueOf(value).Elem())
				val := bp.Get(reflect.ValueOf(value))
				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}

				if column.IsKey() {
					if val.Kind() == reflect.Ptr {
						if val.IsNil() {
							panic(fmt.Sprintf("Value for key property '%s' cannot be nil.", alias))
						}
						val = val.Elem()
					}
					id = val.Interface()

					if criterias != nil {
						criterias = append(criterias, column.Matches(Param(alias)))
					}
					this.SetParameter(alias, id)
				} else if column.IsVersion() {
					if val.Kind() == reflect.Ptr {
						if val.IsNil() {
							panic(fmt.Sprintf("Value for version property '%s' cannot be nil.", alias))
						}
						val = val.Elem()
					}

					ver = val.Int()
					if ver != 0 {
						alias_old := alias + "_old"
						if criterias != nil {
							criterias = append(criterias, column.Matches(Param(alias_old)))
						}
						this.SetParameter(alias_old, ver)
						// increments the version
						this.Set(column, ver+1)
						mustSucceed = true
					}
				} else {
					var isNil bool
					if val.Kind() == reflect.Ptr {
						isNil = val.IsNil()
						if isNil {
							this.Set(column, nil)
						} else {
							val = val.Elem()
						}
					}

					if !isNil {
						v := val.Interface()
						switch T := v.(type) {
						case driver.Valuer:
							value, err := T.Value()
							if err != nil {
								return 0, err
							}
							this.Set(column, value)
						default:
							this.Set(column, v)
						}
					}
				}
			}
		}
	}
	if criterias != nil {
		this.Where(criterias...)
		this.rawSQL = nil
	}

	affectedRows, err := this.Execute()
	if err != nil {
		return 0, err
	}
	if affectedRows == 0 && mustSucceed {
		return 0, dbx.NewOptimisticLockFail("", fmt.Sprintf("Unable to UPDATE record with id=%v and version=%v for table %s",
			id, ver, this.GetTable().GetName()))
	}
	return affectedRows, nil
}

func (this *Update) Execute() (int64, error) {
	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	affectedRows, e := this.DmlBase.dba.Update(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (this *Update) getCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(make([]*Criteria, 0)...)
		}

		sql := this.db.GetTranslator().GetSqlForUpdate(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}

	return this.rawSQL
}

// JOINS ===

// There are no joins in updates. Some databases do implemente them (ex: PostgreSQL) but others do not (ex: FirebirdSQL).
// A standard way is to use EXISTS in the where condition.
// If updating a table with values from other table, use a subquery.
// ex:
// update SOMETABLE a
// set a.Name = (select lower(b.name) from ANOTHERTABLE b where b.id = a.id)
// where EXISTS (select b.name from ANOTHERTABLE b where b.id = a.id and b.name like 'P%')

//// WHERE ===

func (this *Update) Where(restriction ...*Criteria) *Update {
	if len(restriction) > 0 {
		this.DmlBase.where(restriction...)
	}
	return this
}
