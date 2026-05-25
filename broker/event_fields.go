package broker

import (
	"fmt"
	"reflect"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func eventKind(event any) string {
	if event == nil {
		return ""
	}
	typ := reflect.TypeOf(event)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Name() != "" {
		return typ.Name()
	}
	return fmt.Sprintf("%T", event)
}

func eventFields(event any) *collectionmapping.Map[string, string] {
	value, ok := eventStructValue(event)
	if !ok {
		return nil
	}
	return eventStructFields(value)
}

func eventStructValue(event any) (reflect.Value, bool) {
	if event == nil {
		return reflect.Value{}, false
	}
	value := reflect.ValueOf(event)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	return value, true
}

func eventStructFields(value reflect.Value) *collectionmapping.Map[string, string] {
	typ := value.Type()
	fields := collectionmapping.NewMap[string, string]()
	for i := range value.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		text, ok := eventFieldString(value.Field(i))
		if ok {
			fields.Set(field.Name, text)
		}
	}
	if fields.IsEmpty() {
		return nil
	}
	return fields
}

func eventFieldString(value reflect.Value) (string, bool) {
	if !value.IsValid() || !value.CanInterface() {
		return "", false
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", false
		}
		value = value.Elem()
	}
	return fmt.Sprint(value.Interface()), true
}
