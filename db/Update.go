package db

import (
	"github.com/quintans/goSQL/dbx"
	coll "github.com/quintans/toolkit/collections"

	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type PreUpdater interface {
	PreUpdate(store IDb) error
}

type PostUpdater interface {
	PostUpdate(store IDb)
}

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

//Updates all the columns of the table to matching struct fields.
//Returns the number of affected rows
func (this *Update) Submit(instance interface{}) (int64, error) {
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
	var criterias []*Criteria

	if typ == this.lastType {
		mappings = this.lastMappings
	} else {
		mappings = PopulateMapping("", typ)
		criterias = make([]*Criteria, 0)
		this.criteria = nil
		this.lastMappings = mappings
		this.lastType = typ
	}

	var id interface{}
	var ver int64
	var verColumn *Column

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

	for e := this.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		alias := column.GetAlias()
		bp := mappings[alias]
		if bp != nil {
			val := bp.Get(elem)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}

			if column.IsKey() {
				if !val.IsValid() || (val.Kind() == reflect.Ptr && val.IsNil()) {
					return 0, errors.New(fmt.Sprintf("goSQL: Value for key property '%s' cannot be nil.", alias))
				}

				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}
				id = val.Interface()

				if criterias != nil {
					criterias = append(criterias, column.Matches(Param(alias)))
				}
				this.SetParameter(alias, id)
			} else if column.IsVersion() {
				if !val.IsValid() || (val.Kind() == reflect.Ptr && val.IsNil()) {
					panic(fmt.Sprintf("goSQL: Value for version property '%s' cannot be nil.", alias))
				}
				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}

				ver = val.Int()
				// if version is 0 it means an update where optimistic locking is ignored
				if ver != 0 {
					alias_old := alias + "_old"
					if criterias != nil {
						criterias = append(criterias, column.Matches(Param(alias_old)))
					}
					this.SetParameter(alias_old, ver)
					// increments the version
					this.Set(column, ver+1)
					verColumn = column
				}
			} else {
				var marked = useMarks && marks[column.GetAlias()]
				if val.IsValid() && (!useMarks || marked) {
					var isNil bool
					if val.Kind() == reflect.Ptr {
						isNil = val.IsNil()
						if isNil {
							if marked || acceptField(bp.Tag, nil) {
								this.Set(column, nil)
							}
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
							if marked || acceptField(bp.Tag, value) {
								this.Set(column, value)
							}
						default:
							if marked || acceptField(bp.Tag, v) {
								this.Set(column, v)
							}
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

	// pre trigger
	if t, isT := instance.(PreUpdater); isT {
		err := t.PreUpdate(this.GetDb())
		if err != nil {
			return 0, err
		}
	}

	affectedRows, err := this.Execute()
	if err != nil {
		return 0, err
	}

	if verColumn != nil {
		if affectedRows == 0 {
			return 0, dbx.NewOptimisticLockFail(fmt.Sprintf("Optimistic Lock Error: Unable to UPDATE record with id=%v and version=%v for table %s",
				id, ver, this.GetTable().GetName()))
		}

		ver += 1
		bp := mappings[verColumn.GetAlias()]
		bp.Set(elem, reflect.ValueOf(&ver))
	}

	// post trigger
	if t, isT := instance.(PostUpdater); isT {
		t.PostUpdate(this.GetDb())
	}

	if isMarkable {
		markable.Unmark()
	}

	return affectedRows, nil
}

// returns the number of affected rows
func (this *Update) Execute() (int64, error) {
	table := this.GetTable()
	if table.PreUpdateTrigger != nil {
		table.PreUpdateTrigger(this)
	}

	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 1)

	now := time.Now()
	affectedRows, e := this.DmlBase.dba.Update(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 1)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (this *Update) getCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(nil)
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
		this.DmlBase.where(restriction)
	}
	return this
}
