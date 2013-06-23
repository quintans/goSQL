package db

import (
	coll "github.com/quintans/toolkit/collection"

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
func (this *DmlCore) set(col *Column, value interface{}) interface{} {
	token := tokenizeOne(value).Clone().(Tokener)
	this.replaceRaw(token)
	token.SetTableAlias(this.tableAlias)
	// if the column was not yet defined, the sql changed
	val, ok := this.defineParameter(col, token)
	if ok {
		this.rawSQL = nil
	}
	return val
}

func (this *DmlCore) values(vals ...interface{}) {
	if len(this.cols) == 0 {
		panic("Column set is not yet defined!")
	}

	if len(this.cols) != len(vals) {
		panic("The number of defined cols is diferent from the number of passed vals!")
	}

	for k, col := range this.cols {
		this.set(col, vals[k])
	}
}

// defines a new value for column returning if the column should provoque a new sql
func (this *DmlCore) defineParameter(col *Column, value Tokener) (interface{}, bool) {
	if col.GetTable().GetName() != this.table.GetName() {
		panic(col.String() + " does not belong to table " + this.table.String())
	}

	if this.vals == nil {
		this.vals = coll.NewLinkedHashMap()
	}

	old := this.vals.Put(col, value)
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
			val := this.parameters[oldKey]
			// update the old value to the new one
			this.parameters[oldKey] = this.parameters[key]
			// remove the new token
			delete(this.parameters, key)
			// The replace of one param by another should not trigger a new SQL string
			return val, false
		} else if tok.GetOperator() == TOKEN_PARAM {
			// removes the previous token
			delete(this.parameters, tok.GetValue().(string))
		}
	}
	return nil, true
}

func (this *DmlCore) GetValues() coll.Map {
	return this.vals
}
