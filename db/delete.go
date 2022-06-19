package db

import (
	"fmt"
	"reflect"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
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
	this.init(db, table)
	return this
}

func (d *Delete) Alias(alias string) *Delete {
	d.alias(alias)
	return d
}

func (d *Delete) Submit(value interface{}) (int64, error) {
	var mappings map[string]*EntityProperty
	var criterias []*Criteria

	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ == d.lastType {
		mappings = d.lastMappings
	} else {
		var err error
		mappings, err = d.GetDb().PopulateMapping("", typ)
		if err != nil {
			return 0, faults.Wrap(err)
		}
		criterias = make([]*Criteria, 0)
		d.criteria = nil
		d.lastMappings = mappings
		d.lastType = typ
	}

	var mustSucceed bool
	var hasId bool
	var ver int64
	for e := d.table.GetColumns().Enumerator(); e.HasNext(); {
		column := e.Next().(*Column)
		alias := column.GetAlias()
		bp := mappings[alias]
		if bp != nil {
			val := bp.Get(reflect.ValueOf(value))
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}

			if column.IsKey() {
				if !val.IsValid() || (val.Kind() == reflect.Ptr && val.IsNil()) {
					return 0, faults.Errorf("goSQL: Value for key property '%s' cannot be nil.", alias)
				}

				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}
				id := val.Interface()

				if criterias != nil {
					criterias = append(criterias, column.Matches(Param(alias)))
				}
				d.SetParameter(alias, id)
				hasId = true
			} else if column.IsVersion() {
				if !val.IsValid() || (val.Kind() == reflect.Ptr && val.IsNil()) {
					return 0, faults.Errorf("value for version property '%s' cannot be nil.", alias)
				}

				if val.Kind() == reflect.Ptr {
					val = val.Elem()
				}

				ver = val.Int()
				if ver != 0 {
					if criterias != nil {
						criterias = append(criterias, column.Matches(Param(alias)))
					}
					d.SetParameter(alias, ver)
					mustSucceed = true
				}
			}
		}
	}

	if !hasId {
		return 0, faults.Errorf("goSQL: No key field was identified in %s.", typ.String())
	}

	if criterias != nil {
		d.Where(criterias...)
		d.rawSQL = nil
	}

	// pre trigger
	if t, isT := value.(PreDeleter); isT {
		err := t.PreDelete(d.GetDb())
		if err != nil {
			return 0, faults.Wrap(err)
		}
	}

	affectedRows, err := d.Execute()
	if err != nil {
		return 0, faults.Wrap(err)
	}
	if affectedRows == 0 && mustSucceed {
		return 0, dbx.NewOptimisticLockFail(fmt.Sprintf("goSQL: Optimistic Lock Fail when deleting record for %+v", value))
	}

	// post trigger
	if t, isT := value.(PostDeleter); isT {
		t.PostDelete(d.GetDb())
	}
	return affectedRows, nil
}

func (d *Delete) Execute() (int64, error) {
	table := d.GetTable()
	if table.PreDeleteTrigger != nil {
		table.PreDeleteTrigger(d)
	}

	rsql := d.getCachedSql()
	d.debugSQL(rsql.OriSql, 1)

	params, err := rsql.BuildValues(d.DmlBase.parameters)
	if err != nil {
		return 0, faults.Wrap(err)
	}
	affectedRows, e := d.DmlBase.dba.DeleteX(d.db.GetContext(), rsql.Sql, params...)
	if e != nil {
		return 0, e
	}

	return affectedRows, nil
}

func (d *Delete) getCachedSql() *RawSql {
	if d.rawSQL == nil {
		// if the discriminator conditions have not yet been processed, apply them now
		if d.discriminatorCriterias != nil && d.criteria == nil {
			d.DmlBase.where(nil)
		}

		sql := d.db.GetTranslator().GetSqlForDelete(d)
		d.rawSQL = ToRawSql(sql, d.db.GetTranslator())
	}

	return d.rawSQL
}

//// WHERE ===

func (d *Delete) Where(restriction ...*Criteria) *Delete {
	if len(restriction) > 0 {
		d.DmlBase.where(restriction)
	}
	return d
}
