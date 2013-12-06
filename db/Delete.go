package db

import (
	"github.com/quintans/goSQL/dbx"

	"errors"
	"fmt"
	"reflect"
	"time"
)

type PreDeleter interface {
	PreDelete(store IDb) error
}

type PostDeleter interface {
	PostDelete(store IDb) error
}

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
	var hasId bool
	var ver int64
	for e := this.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		if !column.IsVirtual() {
			alias := column.GetAlias()
			bp := mappings[alias]
			if bp != nil {
				val := bp.Get(reflect.ValueOf(value))
				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}

				if column.IsKey() {
					if val.Kind() == reflect.Ptr {
						if val.IsNil() {
							return 0, errors.New(fmt.Sprintf("goSQL: Value for key property '%s' cannot be nil.", alias))
						}
						val = val.Elem()
					}
					id := val.Interface()

					if criterias != nil {
						criterias = append(criterias, column.Matches(Param(alias)))
					}
					this.SetParameter(alias, id)
					hasId = true
				} else if column.IsVersion() {
					if val.Kind() == reflect.Ptr {
						if val.IsNil() {
							return 0, errors.New(fmt.Sprintf("goSQL: Value for version property '%s' cannot be nil.", alias))
						}
						val = val.Elem()
					}

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

	if !hasId {
		return 0, errors.New(fmt.Sprintf("goSQL: No key field was identified in %s.", typ.String()))
	}

	if criterias != nil {
		this.Where(criterias...)
		this.rawSQL = nil
	}

	// pre trigger
	if t, isT := value.(PreDeleter); isT {
		err := t.PreDelete(this.GetDb())
		if err != nil {
			return 0, err
		}
	}

	affectedRows, err := this.Execute()
	if err != nil {
		return 0, err
	}
	if affectedRows == 0 && mustSucceed {
		return 0, dbx.NewOptimisticLockFail(fmt.Sprintf("goSQL: Optimistic Lock Fail when deleting record for %+v", value))
	}

	// post trigger
	if t, isT := value.(PostDeleter); isT {
		t.PostDelete(this.GetDb())
	}
	return affectedRows, nil
}

func (this *Delete) Execute() (int64, error) {
	rsql := this.getCachedSql()
	this.debugSQL(rsql.OriSql, 1)

	now := time.Now()
	affectedRows, e := this.DmlBase.dba.Delete(rsql.Sql, rsql.BuildValues(this.DmlBase.parameters)...)
	this.debugTime(now, 1)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (this *Delete) getCachedSql() *RawSql {
	if this.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if this.discriminatorCriterias != nil && this.criteria == nil {
			this.DmlBase.where(nil)
		}

		sql := this.db.GetTranslator().GetSqlForDelete(this)
		this.rawSQL = ToRawSql(sql, this.db.GetTranslator())
	}

	return this.rawSQL
}

//// WHERE ===

func (this *Delete) Where(restriction ...*Criteria) *Delete {
	if len(restriction) > 0 {
		this.DmlBase.where(restriction)
	}
	return this
}
