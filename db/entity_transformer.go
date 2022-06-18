package db

import (
	"database/sql"
	"reflect"
	"strings"

	"github.com/quintans/faults"
	"github.com/quintans/goSQL/dbx"
	coll "github.com/quintans/toolkit/collections"
	"github.com/quintans/toolkit/ext"
)

type EntityTransformerOverrider interface {
	dbx.IRowTransformer

	PopulateMapping(tableAlias string, typ reflect.Type) (map[string]*EntityProperty, error)
	DiscardIfKeyIsNull() bool
	InitRowData(row []interface{}, properties map[string]*EntityProperty)
	ToEntity(row []interface{}, instance reflect.Value, properties map[string]*EntityProperty, emptyBean *bool) (bool, error)
}

type EntityTransformer struct {
	Overrider    EntityTransformerOverrider
	Query        *Query
	Factory      func() reflect.Value
	Returner     func(val reflect.Value) reflect.Value
	Properties   map[string]*EntityProperty
	TemplateData []interface{}
}

// ensures IRowTransformer interface
var _ dbx.IRowTransformer = &EntityTransformer{}

func NewEntityTransformer(query *Query, instance interface{}) *EntityTransformer {
	return NewEntityFactoryTransformer(query, reflect.TypeOf(instance), nil)
}

func NewEntityFactoryTransformer(query *Query, typ reflect.Type, returner func(val reflect.Value) reflect.Value) *EntityTransformer {
	this := new(EntityTransformer)
	this.Overrider = this

	// used as super by extenders
	this.Super(query, createFactory(typ), returner)

	return this
}

func createFactory(typ reflect.Type) func() reflect.Value {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		return func() reflect.Value {
			return reflect.New(typ)
		}
	} else {
		return func() reflect.Value {
			return reflect.Zero(typ)
		}
	}
}

func (e *EntityTransformer) Super(query *Query, factory func() reflect.Value, returner func(val reflect.Value) reflect.Value) {
	e.Query = query
	e.Factory = factory
	e.Returner = returner
}

func (e *EntityTransformer) BeforeAll() coll.Collection {
	return coll.NewArrayList()
}

//	 Populates the mapping with instance properties, using "tableAlias.propertyName" as key
//   Only properties of a type present in the query are mapped
//	 param mappings: instance of Map
//	 param tableAlias: The table alias. If <code>nil</code> the mapping key is only "propertyName"
//	 param type: The entity class
func (e *EntityTransformer) PopulateMapping(tableAlias string, typ reflect.Type) (map[string]*EntityProperty, error) {
	prefix := tableAlias
	if tableAlias != "" {
		prefix = tableAlias + "."
	}

	mappings, err := e.Query.GetDb().PopulateMapping(prefix, typ)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	// Matches the columns with the bean properties
	for idx, token := range e.Query.Columns {
		ta := token.GetAlias()

		var bp *EntityProperty = nil
		ch, ok := token.(*ColumnHolder)
		if tableAlias == "" {
			if ta == "" && ok {
				ta = ch.GetColumn().GetAlias()
			}
			bp = mappings[prefix+capFirst(ta)]
		} else if ok {
			if tableAlias == token.GetPseudoTableAlias() {
				bp = mappings[prefix+capFirst(ch.GetColumn().GetAlias())]
				if e.Overrider.DiscardIfKeyIsNull() && bp != nil {
					bp.Key = ch.GetColumn().IsKey()
				}
			}
		}

		if bp != nil {
			bp.Position = idx + 1
		}
	}

	return mappings, nil
}

func capFirst(name string) string {
	return strings.ToUpper(name[:1]) + name[1:]
}

// can be overriden
func (e *EntityTransformer) DiscardIfKeyIsNull() bool {
	return false
}

func (e *EntityTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	if instance != nil {
		result.Add(instance)
	}
}

func (e *EntityTransformer) AfterAll(result coll.Collection) {
}

func (e *EntityTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	val := e.Factory()

	if e.Properties == nil {
		var err error
		e.Properties, err = e.Overrider.PopulateMapping("", val.Type())
		if err != nil {
			return nil, faults.Wrap(err)
		}
	}

	if e.TemplateData == nil {
		// creates the array with all the types returned by the query
		// using the entity properties as reference for instantiating the types
		cols, err := rows.Columns()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		length := len(cols)
		e.TemplateData = make([]interface{}, length)
		// set default for unused columns, in case of a projection of a result
		// with more columns than the attributes of the destination struct
		for i := 0; i < len(e.TemplateData); i++ {
			e.TemplateData[i] = &ext.Any{}
		}
		// instantiate all target types
		e.Overrider.InitRowData(e.TemplateData, e.Properties)
	}

	// makes a copy
	rowData := make([]interface{}, len(e.TemplateData), cap(e.TemplateData))
	copy(rowData, e.TemplateData)

	// Scan result set
	if err := rows.Scan(rowData...); err != nil {
		return nil, faults.Wrap(err)
	}

	if _, err := e.Overrider.ToEntity(rowData, val, e.Properties, nil); err != nil {
		return nil, faults.Wrap(err)
	}

	instance := val.Interface()
	// post trigger
	if t, isT := instance.(PostRetriever); isT {
		t.PostRetrieve(e.Query.GetDb())
	}

	if e.Returner == nil {
		return instance, nil
	} else {
		v := e.Returner(val)
		if v.IsValid() {
			return v.Interface(), nil
		}
	}
	return nil, nil
}

func (e *EntityTransformer) InitRowData(
	row []interface{},
	properties map[string]*EntityProperty,
) {
	// instanciate
	for _, bp := range properties {
		if bp.Position > 0 {
			position := bp.Position
			var ptr interface{}
			if bp.converter != nil {
				ptr = bp.converter.FromDbInstance()
			} else {
				ptr = bp.New()
			}
			row[position-1] = ptr
		}
	}
}

// returns if the entity was valid
func (e *EntityTransformer) ToEntity(
	row []interface{},
	instance reflect.Value,
	properties map[string]*EntityProperty,
	emptyBean *bool,
) (bool, error) {
	for _, bp := range properties {
		if bp.Position > 0 {
			position := bp.Position
			value, err := bp.ConvertFromDb(row[position-1])
			if err != nil {
				return false, faults.Wrap(err)
			}

			isPtr := false
			v := reflect.ValueOf(value)
			if v.Kind() == reflect.Ptr {
				isPtr = true
				v = v.Elem()
			}
			ok := bp.Set(instance, v)
			// if it was set being a pointer or it was a non zero value, then it is not empty
			if ok && (isPtr || v.IsValid()) && emptyBean != nil {
				*emptyBean = false
			}
			// if property is a key, and it was not set or it's not a pointer and is a zero value, return invalid
			if bp.Key && (!ok || !isPtr && !v.IsValid()) {
				// if any key is nil or has zero value, the bean is nil. ex: a bean coming from a outer join
				return false, nil
			}
		}
	}

	return true, nil
}
