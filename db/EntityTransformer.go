package db

import (
	"strings"

	"github.com/quintans/goSQL/dbx"
	coll "github.com/quintans/toolkit/collections"
	. "github.com/quintans/toolkit/ext"

	"database/sql"
	"reflect"
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

func (this *EntityTransformer) Super(query *Query, factory func() reflect.Value, returner func(val reflect.Value) reflect.Value) {
	this.Query = query
	this.Factory = factory
	this.Returner = returner
}

func (this *EntityTransformer) BeforeAll() coll.Collection {
	return coll.NewArrayList()
}

//	 Populates the mapping with instance properties, using "tableAlias.propertyName" as key
//   Only properties of a type present in the query are mapped
//	 param mappings: instance of Map
//	 param tableAlias: The table alias. If <code>nil</code> the mapping key is only "propertyName"
//	 param type: The entity class
func (this *EntityTransformer) PopulateMapping(tableAlias string, typ reflect.Type) (map[string]*EntityProperty, error) {
	prefix := tableAlias
	if tableAlias != "" {
		prefix = tableAlias + "."
	}

	mappings, err := PopulateMapping(prefix, typ, this.Query.GetDb().GetTranslator())
	if err != nil {
		return nil, err
	}

	// Matches the columns with the bean properties
	for idx, token := range this.Query.Columns {
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
				if this.Overrider.DiscardIfKeyIsNull() && bp != nil {
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
func (this *EntityTransformer) DiscardIfKeyIsNull() bool {
	return false
}

func (this *EntityTransformer) OnTransformation(result coll.Collection, instance interface{}) {
	if instance != nil {
		result.Add(instance)
	}
}

func (this *EntityTransformer) AfterAll(result coll.Collection) {
}

func (this *EntityTransformer) Transform(rows *sql.Rows) (interface{}, error) {
	val := this.Factory()

	if this.Properties == nil {
		var err error
		this.Properties, err = this.Overrider.PopulateMapping("", val.Type())
		if err != nil {
			return nil, err
		}
	}

	if this.TemplateData == nil {
		// creates the array with all the types returned by the query
		// using the entity properties as reference for instantiating the types
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		length := len(cols)
		this.TemplateData = make([]interface{}, length, length)
		// set default for unused columns, in case of a projection of a result
		// with more columns than the attributes of the destination struct
		for i := 0; i < len(this.TemplateData); i++ {
			this.TemplateData[i] = &Any{}
		}
		// instanciate all target types
		this.Overrider.InitRowData(this.TemplateData, this.Properties)
	}
	// makes a copy
	rowData := make([]interface{}, len(this.TemplateData), cap(this.TemplateData))
	copy(rowData, this.TemplateData)

	// Scan result set
	if err := rows.Scan(rowData...); err != nil {
		return nil, err
	}

	if _, err := this.Overrider.ToEntity(rowData, val, this.Properties, nil); err != nil {
		return nil, err
	}

	instance := val.Interface()
	// post trigger
	if t, isT := instance.(PostRetriver); isT {
		t.PostRetrive(this.Query.GetDb())
	}

	if this.Returner == nil {
		return instance, nil
		/*
			if H, isH := instance.(tk.Hasher); isH {
				return H, nil
			}
		*/
	} else {
		v := this.Returner(val)
		if v.IsValid() {
			return v.Interface(), nil
		}
	}

	return nil, nil
}

func (this *EntityTransformer) InitRowData(
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
func (this *EntityTransformer) ToEntity(
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
				return false, err
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
