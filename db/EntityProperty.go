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
	Omit      bool
	converter Converter
	getter    getter
	setter    setter
}

func (this *EntityProperty) New() interface{} {
	return reflect.New(this.Type).Interface()
}

func (this *EntityProperty) IsMany() bool {
	return this.InnerType != nil
}

type setter func(instance reflect.Value) reflect.Value

func makeSetter(previous setter, fieldname string) setter {
	return func(instance reflect.Value) reflect.Value {
		if previous != nil {
			instance = previous(instance)
		}

		if instance.Kind() == reflect.Ptr {
			if instance.IsNil() {
				t := instance.Type().Elem()
				val := reflect.New(t)
				instance.Set(val)
			}
			instance = instance.Elem()
		}
		instance = reflect.Indirect(instance).FieldByName(fieldname)
		if !instance.CanSet() {
			// Cheat: writting to unexported fields
			instance = reflect.NewAt(instance.Type(), unsafe.Pointer(instance.UnsafeAddr())).Elem()
		}
		return instance
	}
}

// Do not set nil values.
// If value is nil it will return false, otherwise returns true
func (this *EntityProperty) Set(instance reflect.Value, value reflect.Value) bool {
	// do not set nil values
	if value.Kind() != reflect.Ptr || !value.IsNil() {
		field := this.setter(instance)

		if field.Kind() == reflect.Ptr || field.Kind() == reflect.Slice || field.Kind() == reflect.Array {
			field.Set(value)
		} else {
			field.Set(value.Elem())
		}
		return true
	}
	return false
}

type getter func(instance reflect.Value) reflect.Value

func makeGetter(previous getter, fieldname string) getter {
	return func(instance reflect.Value) reflect.Value {
		if previous != nil {
			instance = previous(instance)
		}
		if instance.Kind() == reflect.Ptr {
			instance = instance.Elem()
		}
		return instance.FieldByName(fieldname)
	}
}

func (this *EntityProperty) Get(instance reflect.Value) reflect.Value {
	return this.getter(instance)
}

func PopulateMappingOf(prefix string, m interface{}, translator Translator) (map[string]*EntityProperty, error) {
	return PopulateMapping(prefix, reflect.TypeOf(m), translator)
}

func PopulateMapping(prefix string, typ reflect.Type, translator Translator) (map[string]*EntityProperty, error) {
	// create an attribute data structure as a map of types keyed by a string.
	attrs := make(map[string]*EntityProperty)

	err := walkTreeStruct(prefix, typ, attrs, translator, nil, nil)

	return attrs, err
}

func walkTreeStruct(prefix string, typ reflect.Type, attrs map[string]*EntityProperty, translator Translator, prevGetter getter, prevSetter setter) error {
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
		var omit, embeded bool
		sqlVal := p.Tag.Get(sqlKey)
		if sqlVal != "" {
			splits := strings.Split(sqlVal, ",")
			for _, v := range splits {
				switch v {
				case sqlOmitionVal:
					omit = true
				case sqlEmbededVal:
					embeded = true
				}
			}
		}
		if p.Anonymous {
			if err := walkTreeStruct(prefix, p.Type, attrs, translator, prevGetter, prevSetter); err != nil {
				return err
			}
		} else if embeded {
			nextGetter := makeGetter(prevGetter, p.Name)
			nextSetter := makeSetter(prevSetter, p.Name)
			if err := walkTreeStruct(prefix, p.Type, attrs, translator, nextGetter, nextSetter); err != nil {
				return err
			}
		} else {
			ep := &EntityProperty{}
			ep.getter = makeGetter(prevGetter, p.Name)
			ep.setter = makeSetter(prevSetter, p.Name)

			key := strings.ToUpper(p.Name[:1]) + p.Name[1:]
			if prefix != "" {
				key = prefix + key
			}
			attrs[key] = ep
			ep.FieldName = p.Name
			ep.Omit = omit
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
