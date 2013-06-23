package db

import (
	"fmt"
	"github.com/quintans/goSQL/dbx"
	"reflect"
	"time"
)

type Delete struct {
	DmlCore
}

func NewDelete(db IDb, table *Table) *Delete {
	this := new(Delete)
	this.Super(db, table)
	return this
}

func (this *Delete) Alias(alias string) *Delete {
	this.alias(alias)
	return this
}

func (this *Delete) Submit(value interface{}) (int64, error) {
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
				val := bp.Get(reflect.ValueOf(value).Elem())
				v := val.Interface()

				if column.IsKey() {
					if v == nil {
						panic(fmt.Sprintf("Value for key property '%s' cannot be nil.", alias))
					}

					if criterias != nil {
						criterias = append(criterias, column.Matches(Param(alias)))
					}
					this.SetParameter(alias, v)
				} else if column.IsVersion() {
					ver = val.Int()
					if ver != 0 {
						if criterias != nil {
							criterias = append(criterias, column.Matches(Param(alias)))
						}
						this.SetParameter(alias, ver)
						mustSucceed = true
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
		return 0, dbx.NewOptimisticLockFail("", fmt.Sprintf("Unable to DELETE record with id=%v and version=%v for table %s",
			id, ver, this.GetTable().GetName()))
	}
	return affectedRows, nil
}

func (this *Delete) Execute() (int64, error) {
	rsql := this.GetCachedSql()
	this.debugSQL(rsql.OriSql)

	now := time.Now()
	affectedRows, e := this.DmlBase.dba.Delete(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (this *Delete) GetCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(make([]*Criteria, 0)...)
		}

		sql := this.db.GetTranslator().GetSqlForDelete(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}

	return this.rawSQL
}

// JOINS ===
func (this *Delete) Inner(associations ...*Association) *Delete {
	this.DmlBase.inner(associations...)
	return this
}

func (this *Delete) Join() *Delete {
	// resets path
	this.DmlBase.join()
	return this
}

//  indicates that the path should be used to join only
//
// param endAlias
// return
func (this *Delete) JoinAs(endAlias string) *Delete {
	this.DmlBase.joinAs(endAlias)
	return this
}

//  Executes an inner join with several associations
//
// param associations
// return
func (this *Delete) InnerJoin(associations ...*Association) *Delete {
	this.DmlBase.innerJoin(associations...)
	return this
}

//  criteria to apply to the previous association
//
// param criteria: restriction
// return
func (this *Delete) On(criteria *Criteria) *Delete {
	this.DmlBase.on(criteria)
	return this
}

//// WHERE ===

func (this *Delete) Where(restriction ...*Criteria) *Delete {
	if len(restriction) > 0 {
		this.DmlBase.where(restriction...)
	}
	return this
}
