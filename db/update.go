package db

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
	coll "github.com/quintans/toolkit/collections"
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

func (u *Update) Alias(alias string) *Update {
	u.alias(alias)
	return u
}

func (u *Update) Set(col *Column, value interface{}) *Update {
	u.DmlCore.set(col, value)
	return u
}

func (u *Update) Columns(columns ...*Column) *Update {
	u.cols = columns
	return u
}

func (u *Update) Values(vals ...interface{}) *Update {
	u.DmlCore.values(vals...)
	return u
}

// Updates all the columns of the table to matching struct fields.
// Returns the number of affected rows
func (u *Update) Submit(instance interface{}) (int64, error) {
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
		return 0, faults.New("the argument must be a struct pointer")
	}

	var mappings map[string]*EntityProperty
	var criterias []*Criteria

	if typ == u.lastType {
		mappings = u.lastMappings
	} else {
		var err error
		mappings, err = u.GetDb().PopulateMapping("", typ)
		if err != nil {
			return 0, err
		}
		criterias = make([]*Criteria, 0)
		u.criteria = nil
		u.lastMappings = mappings
		u.lastType = typ
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

	for e := u.table.GetColumns().Enumerator(); e.HasNext(); {
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
					return 0, faults.Errorf("value for key property '%s' cannot be nil.", alias)
				}

				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}
				id = val.Interface()

				if criterias != nil {
					criterias = append(criterias, column.Matches(Param(alias)))
				}
				u.SetParameter(alias, id)
			} else if column.IsVersion() {
				if !val.IsValid() || (val.Kind() == reflect.Ptr && val.IsNil()) {
					faults.Errorf("value for version property '%s' cannot be nil.", alias)
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
					u.SetParameter(alias_old, ver)
					// increments the version
					u.Set(column, ver+1)
					verColumn = column
				}
			} else {
				marked := useMarks && marks[column.GetAlias()]
				if val.IsValid() && (!useMarks || marked) {
					var isNil bool
					if val.Kind() == reflect.Ptr {
						isNil = val.IsNil()
						if isNil {
							if marked || acceptField(bp.Omit, nil) {
								u.Set(column, nil)
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
							value, err = bp.ConvertToDb(value)
							if err != nil {
								return 0, err
							}
							if marked || acceptField(bp.Omit, value) {
								u.Set(column, value)
							}
						default:
							if marked || acceptField(bp.Omit, v) {
								var err error
								v, err = bp.ConvertToDb(v)
								if err != nil {
									return 0, err
								}
								u.Set(column, v)
							}
						}
					}
				}
			}
		}
	}
	if criterias != nil {
		u.Where(criterias...)
		u.rawSQL = nil
	}

	// pre trigger
	if t, isT := instance.(PreUpdater); isT {
		err := t.PreUpdate(u.GetDb())
		if err != nil {
			return 0, err
		}
	}

	affectedRows, err := u.Execute()
	if err != nil {
		return 0, err
	}

	if verColumn != nil {
		if affectedRows == 0 {
			return 0, dbx.NewOptimisticLockFail(fmt.Sprintf("Optimistic Lock Error: Unable to UPDATE record with id=%v and version=%v for table %s",
				id, ver, u.GetTable().GetName()))
		}

		ver++
		bp := mappings[verColumn.GetAlias()]
		bp.Set(elem, reflect.ValueOf(&ver))
	}

	// post trigger
	if t, isT := instance.(PostUpdater); isT {
		t.PostUpdate(u.GetDb())
	}

	if isMarkable {
		markable.Unmark()
	}

	return affectedRows, nil
}

// returns the number of affected rows
func (u *Update) Execute() (int64, error) {
	table := u.GetTable()
	if table.PreUpdateTrigger != nil {
		table.PreUpdateTrigger(u)
	}

	rsql := u.getCachedSql()
	u.debugSQL(rsql.OriSql, 1)

	now := time.Now()
	params, err := rsql.BuildValues(u.DmlBase.parameters)
	if err != nil {
		return 0, err
	}
	affectedRows, e := u.DmlBase.dba.Update(rsql.Sql, params...)
	u.debugTime(now, 1)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (u *Update) getCachedSql() *RawSql {
	if u.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if u.discriminatorCriterias != nil && u.criteria == nil {
			u.DmlBase.where(nil)
		}

		sql := u.db.GetTranslator().GetSqlForUpdate(u)
		u.rawSQL = ToRawSql(sql, u.db.GetTranslator())
	}

	return u.rawSQL
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

func (u *Update) Where(restriction ...*Criteria) *Update {
	if len(restriction) > 0 {
		u.DmlBase.where(restriction)
	}
	return u
}
