package db

import (
	"github.com/quintans/goSQL/dbx"
	tk "github.com/quintans/toolkit"
	coll "github.com/quintans/toolkit/collection"
	. "github.com/quintans/toolkit/ext"

	"database/sql"
	"reflect"
)

type EntityTransformerOverrider interface {
	dbx.IRowTransformer

	PopulateMapping(tableAlias string, typ reflect.Type) map[string]*EntityProperty
	DiscardIfKeyIsNull() bool
	InitRowData(row []interface{}, properties map[string]*EntityProperty)
	ToEntity(row []interface{}, instance reflect.Value, properties map[string]*EntityProperty) (bool, error)
}

type EntityTransformer struct {
	Overrider              EntityTransformerOverrider
	Query                  *Query
	Factory                func() reflect.Value
	Returner               reflect.Value
	PaginationColumnOffset int
	Properties             map[string]*EntityProperty
	TemplateData           []interface{}
}

// ensures IRowTransformer interface
var _ dbx.IRowTransformer = &EntityTransformer{}

func NewEntityTransformer(query *Query, instance interface{}) *EntityTransformer {
	return NewEntityFactoryTransformer(query, reflect.TypeOf(instance), reflect.Value{})
}

func NewEntityFactoryTransformer(query *Query, typ reflect.Type, returner reflect.Value) *EntityTransformer {
	this := new(EntityTransformer)
	this.Overrider = this

	// used as super by extenders
	this.Super(query, createFactory(typ), returner)

	return this
}

func createFactory(typ reflect.Type) func() reflect.Value {
	var isPtr bool
	if typ.Kind() == reflect.Ptr {
		isPtr = true
		typ = typ.Elem()
	}

	return func() reflect.Value {
		if isPtr {
			return reflect.New(typ)
		}
		return reflect.Zero(typ)
	}
}

func (this *EntityTransformer) Super(query *Query, factory func() reflect.Value, returner reflect.Value) {
	this.Query = query
	this.Factory = factory
	this.Returner = returner

	this.PaginationColumnOffset = query.GetDb().GetTranslator().PaginationColumnOffset(query)
}

func (this *EntityTransformer) BeforeAll() coll.Collection {
	return coll.NewArrayList()
}

//	 Populates the mapping with instance properties, using "tableAlias.propertyName" as key
//   Only properties of a type present in the query are mapped
//	 param mappings: instance of Map
//	 param tableAlias: The table alias. If <code>nil</code> the mapping key is only "propertyName"
//	 param type: The entity class
func (this *EntityTransformer) PopulateMapping(tableAlias string, typ reflect.Type) map[string]*EntityProperty {
	prefix := tableAlias
	if tableAlias != "" {
		prefix = tableAlias + "."
	}

	mappings := PopulateMapping(prefix, typ)

	// Matches the columns with the bean properties
	for idx, token := range this.Query.Columns {
		ta := token.GetAlias()

		var bp *EntityProperty = nil
		ch, ok := token.(*ColumnHolder)
		if tableAlias == "" {
			if ta == "" && ok {
				ta = ch.GetColumn().GetAlias()
			}
			bp, _ = mappings[ta]
		} else if ok {
			if tableAlias == ch.GetVirtualTableAlias() {
				bp, _ = mappings[prefix+ch.GetColumn().GetAlias()]
				if this.Overrider.DiscardIfKeyIsNull() && bp != nil {
					bp.Key = ch.GetColumn().IsKey()
				}
			}
		}

		if bp != nil {
			bp.Position = idx + 1
		}
	}

	return mappings
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
	instance := val.Interface()

	if this.Properties == nil {
		this.Properties = this.Overrider.PopulateMapping("", val.Type())
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

	if _, err := this.Overrider.ToEntity(rowData, val, this.Properties); err != nil {
		return nil, err
	}

	if this.Returner.Kind() == reflect.Invalid {
		if H, isH := instance.(tk.Hasher); isH {
			return H, nil
		}
	} else {
		this.Returner.Call([]reflect.Value{val})
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
			position := bp.Position + this.PaginationColumnOffset
			ptr := bp.New().Interface()
			row[position-1] = ptr
		}
	}
}

// returns if the entity was valid
func (this *EntityTransformer) ToEntity(
	row []interface{},
	instance reflect.Value,
	properties map[string]*EntityProperty,
) (bool, error) {

	//fmt.Printf("%s\n", typ)
	//for _, v := range row {
	//	fmt.Printf("%T %+v\n", v, v)
	//}

	for _, bp := range properties {
		if bp.Position > 0 {
			position := bp.Position + this.PaginationColumnOffset
			value := row[position-1]
			if value != nil {
				v := reflect.ValueOf(value)
				if v.Kind() == reflect.Ptr {
					v = v.Elem()
				}
				bp.Set(instance, v)
			} else if bp.Key { // if property is a key, check if it is nil
				// if any key is nil, the bean is nil. ex: a bean coming from a outer join
				return false, nil
			}
		}
	}

	return true, nil
}
