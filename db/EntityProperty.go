package db

import (
	"fmt"
	"reflect"
)

type EntityProperty struct {
	FieldName string
	Position  int
	Type      reflect.Type
	InnerType reflect.Type
	Key       bool
}

func (this *EntityProperty) New() reflect.Value {
	return reflect.New(this.Type)
}

func (this *EntityProperty) IsMany() bool {
	return this.InnerType != nil
}

func (this *EntityProperty) Set(instance reflect.Value, value reflect.Value) {
	// do not set nil values
	if value.Kind() != reflect.Ptr || !value.IsNil() {
		fmt.Println("===> instance:", instance.String())
		if instance.Kind() == reflect.Ptr {
			instance = instance.Elem()
		}
		field := instance.FieldByName(this.FieldName)
		if field.Kind() == reflect.Ptr {
			ptr := this.New().Elem()
			ptr.Set(value)
			field.Set(ptr)
		} else {
			field.Set(value)
		}
	}
}

func (this *EntityProperty) Get(instance reflect.Value) reflect.Value {
	if instance.Kind() == reflect.Ptr {
		instance = instance.Elem()
	}
	return instance.FieldByName(this.FieldName)
}

func PopulateMappingOf(prefix string, m interface{}) map[string]*EntityProperty {
	return PopulateMapping(prefix, reflect.TypeOf(m))
}

func PopulateMapping(prefix string, typ reflect.Type) map[string]*EntityProperty {
	// create an attribute data structure as a map of types keyed by a string.
	attrs := make(map[string]*EntityProperty)

	walkTreeStruct(prefix, typ, attrs)

	return attrs
}

func walkTreeStruct(prefix string, typ reflect.Type, attrs map[string]*EntityProperty) {
	// if a pointer to a struct is passed, get the type of the dereferenced object
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Only structs are supported so return an empty result if the passed object
	// isn't a struct
	if typ.Kind() != reflect.Struct {
		return
	}

	// loop through the struct's fields and set the map
	for i := 0; i < typ.NumField(); i++ {
		p := typ.Field(i)
		// no package path equals to exported field
		if p.PkgPath == "" {
			if p.Anonymous {
				walkTreeStruct(prefix, p.Type, attrs)
			} else {
				ep := new(EntityProperty)
				var key string
				if prefix == "" {
					key = p.Name
				} else {
					key = prefix + p.Name
				}
				attrs[key] = ep
				ep.FieldName = p.Name
				//if p.Type.Kind() == reflect.Ptr {
				//	ep.Type = p.Type.Elem()
				//} else {
				//	ep.Type = p.Type
				//}
				// we want pointers too
				ep.Type = p.Type

				if p.Type.Kind() == reflect.Slice || p.Type.Kind() == reflect.Array {
					ep.InnerType = p.Type.Elem()
				}
			}
		}
	}
}
