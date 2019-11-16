package db

import (
	"reflect"
	"strings"
	"unsafe"

	"github.com/quintans/toolkit/faults"
)

type EntityProperty struct {
	FieldName string
	Position  int
	Type      reflect.Type
	InnerType reflect.Type
	Key       bool
	Tag       reflect.StructTag
	converter Converter
}

func (this *EntityProperty) New() interface{} {
	return reflect.New(this.Type).Interface()
}

func (this *EntityProperty) IsMany() bool {
	return this.InnerType != nil
}

// Do not set nil values.
// If value is nil it will return false, otherwise returns true
func (this *EntityProperty) Set(instance reflect.Value, value reflect.Value) bool {
	// do not set nil values
	if value.Kind() != reflect.Ptr || !value.IsNil() {
		if instance.Kind() == reflect.Ptr {
			instance = instance.Elem()
		}
		field := instance.FieldByName(this.FieldName)
		if !field.CanSet() {
			// Cheat: writting to unexported fields
			field = reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
		}
		if field.Kind() == reflect.Ptr || field.Kind() == reflect.Slice || field.Kind() == reflect.Array {
			field.Set(value)
		} else {
			field.Set(value.Elem())
		}
		return true
	}
	return false
}

func (this *EntityProperty) Get(instance reflect.Value) reflect.Value {
	if instance.Kind() == reflect.Ptr {
		instance = instance.Elem()
	}
	return instance.FieldByName(this.FieldName)
}

func PopulateMappingOf(prefix string, m interface{}, translator Translator) (map[string]*EntityProperty, error) {
	return PopulateMapping(prefix, reflect.TypeOf(m), translator)
}

func PopulateMapping(prefix string, typ reflect.Type, translator Translator) (map[string]*EntityProperty, error) {
	// create an attribute data structure as a map of types keyed by a string.
	attrs := make(map[string]*EntityProperty)

	err := walkTreeStruct(prefix, typ, attrs, translator)

	return attrs, err
}

func walkTreeStruct(prefix string, typ reflect.Type, attrs map[string]*EntityProperty, translator Translator) error {
	// if a pointer to a struct is passed, get the type of the dereferenced object
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Only structs are supported so return an empty result if the passed object
	// isn't a struct
	if typ.Kind() != reflect.Struct {
		return nil
	}

	// loop through the struct's fields and set the map
	for i := 0; i < typ.NumField(); i++ {
		p := typ.Field(i)
		if p.Anonymous {
			walkTreeStruct(prefix, p.Type, attrs, translator)
		} else {
			ep := &EntityProperty{}
			key := strings.ToUpper(p.Name[:1]) + p.Name[1:]
			if prefix != "" {
				key = prefix + key
			}
			attrs[key] = ep
			ep.FieldName = p.Name
			ep.Tag = p.Tag
			cn := p.Tag.Get(ConverterTag)
			if cn != "" {
				c := translator.GetConverter(cn)
				if c == nil {
					return faults.New("Converter %s is not registered", cn)
				}
				ep.converter = c
			}

			// we want pointers. only pointer are addressable
			if p.Type.Kind() == reflect.Ptr || p.Type.Kind() == reflect.Slice || p.Type.Kind() == reflect.Array {
				ep.Type = p.Type
			} else {
				ep.Type = reflect.PtrTo(p.Type)
			}

			if p.Type.Kind() == reflect.Slice || p.Type.Kind() == reflect.Array {
				ep.InnerType = p.Type.Elem()
			}
		}
	}
	return nil
}

const ConverterTag = "converter"

func (bp *EntityProperty) ConvertFromDb(value interface{}) (interface{}, error) {
	if bp.converter == nil {
		return value, nil
	}
	return bp.converter.FromDb(value)
}

func (bp *EntityProperty) ConvertToDb(value interface{}) (interface{}, error) {
	if bp.converter == nil {
		return value, nil
	}
	return bp.converter.ToDb(value)
}
