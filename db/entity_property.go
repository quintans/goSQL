package db

import (
	"reflect"
	"unsafe"
)

type EntityProperty struct {
	StructProperty
	Position int
}

type StructProperty struct {
	Type      reflect.Type
	InnerType reflect.Type
	Key       bool
	Omit      bool
	converter Converter
	getter    getter
	setter    setter
}

func (e *EntityProperty) New() interface{} {
	return reflect.New(e.Type).Interface()
}

func (e *EntityProperty) IsMany() bool {
	return e.InnerType != nil
}

type setter func(instance reflect.Value) reflect.Value

func makeSetter(index []int) setter {
	return func(instance reflect.Value) reflect.Value {
		for _, x := range index {
			if instance.Kind() == reflect.Ptr {
				if instance.IsNil() {
					t := instance.Type().Elem()
					val := reflect.New(t)
					instance.Set(val)
				}
				instance = instance.Elem()
			}

			instance = reflect.Indirect(instance).Field(x)
			if !instance.CanSet() {
				// Cheat: writting to unexported fields
				instance = reflect.NewAt(instance.Type(), unsafe.Pointer(instance.UnsafeAddr())).Elem()
			}
		}

		return instance
	}
}

// Do not set nil values.
// If value is nil it will return false, otherwise returns true
func (e *EntityProperty) Set(instance reflect.Value, value reflect.Value) bool {
	// do not set nil values
	if value.Kind() != reflect.Ptr || !value.IsNil() {
		field := e.setter(instance)

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

func makeGetter(index []int) getter {
	return func(instance reflect.Value) reflect.Value {
		if instance.Kind() == reflect.Ptr {
			instance = instance.Elem()
		}
		instance = reflect.Indirect(instance).FieldByIndex(index)
		if !instance.CanSet() && instance.CanAddr() {
			// Cheat: writting to unexported fields
			instance = reflect.NewAt(instance.Type(), unsafe.Pointer(instance.UnsafeAddr())).Elem()
		}
		return instance
	}
}

func (e *EntityProperty) Get(instance reflect.Value) reflect.Value {
	return e.getter(instance)
}

const converterTag = "converter="

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
