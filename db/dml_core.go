package db

import (
	"github.com/quintans/faults"
	coll "github.com/quintans/toolkit/collections"

	"reflect"
)

type DmlCore struct {
	DmlBase

	lastType     reflect.Type
	lastMappings map[string]*EntityProperty
	vals         coll.Map
	cols         []*Column
}

// Sets the value by defining a parameter with the column alias.
// This values can be raw values or more elaborated values like
// UPPER(t0.Column) or AUTO(t0.ID)
//
// param col: The column
// param value: The value to set
// return this
func (d *DmlCore) set(col *Column, value interface{}) (interface{}, error) {
	token := tokenizeOne(value)
	d.replaceRaw(token)
	token.SetTableAlias(d.tableAlias)
	// if the column was not yet defined, the sql changed
	val, ok, err := d.defineParameter(col, token)
	if err != nil {
		return nil, err
	}
	if ok {
		d.rawSQL = nil
	}
	return val, nil
}

func (d *DmlCore) values(vals ...interface{}) error {
	if len(d.cols) == 0 {
		return faults.New("Column set is not yet defined!")
	}

	if len(d.cols) != len(vals) {
		return faults.New("The number of defined cols is diferent from the number of passed vals!")
	}

	for k, col := range d.cols {
		d.set(col, vals[k])
	}

	return nil
}

// defines a new value for column returning if the column should provoque a new sql
func (d *DmlCore) defineParameter(col *Column, value Tokener) (interface{}, bool, error) {
	if col.GetTable().GetName() != d.table.GetName() {
		return nil, false, faults.Errorf("%s does not belong to table %s", col, d.table)
	}

	if d.vals == nil {
		d.vals = coll.NewLinkedHashMap()
	}

	old := d.vals.Put(col, value)
	// if it is a parameter remove it
	if old != nil {
		tok := old.(Tokener)
		if value.GetOperator() == TOKEN_PARAM && tok.GetOperator() == TOKEN_PARAM {
			/*
				Replace one param by another
			*/
			oldKey := tok.GetValue().(string)
			key := value.GetValue().(string)
			// change the new param name to the old param name
			value.SetValue(tok.GetValue())
			val := d.parameters[oldKey]
			// update the old value to the new one
			d.parameters[oldKey] = d.parameters[key]
			// remove the new token
			delete(d.parameters, key)
			// The replace of one param by another should not trigger a new SQL string
			return val, false, nil
		} else if tok.GetOperator() == TOKEN_PARAM {
			// removes the previous token
			delete(d.parameters, tok.GetValue().(string))
		}
	}
	return nil, true, nil
}

func (d *DmlCore) GetValues() coll.Map {
	return d.vals
}
