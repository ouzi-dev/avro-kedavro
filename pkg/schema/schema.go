package schema

import "fmt"

type FieldType int

const (
	Unknown   FieldType = 0
	Primitive FieldType = 1
	Union     FieldType = 2
)

type Field struct {
	Name         string
	Type         FieldType
	TypeValue    interface{}
	LogicalType  string
	Fields       []interface{}
	HasDefault   bool
	DefaultValue interface{}
}

func ParseSchemaField(f interface{}) (*Field, error) {
	fieldMap, ok := f.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("field not valid as map[string]interface{}: %v", f)
	}
	name, ok := fieldMap["name"].(string)
	if !ok || len(name) == 0 {
		return nil, fmt.Errorf("field name is required: %v", f)
	}

	typeValue, ok := fieldMap["type"]
	if !ok || typeValue == nil {
		return nil, fmt.Errorf("field type is required: %v", f)
	}

	var fieldType FieldType

	switch t := typeValue.(type) {
	case string:
		fieldType = Primitive
	case []interface{}:
		fieldType = Union
	default:
		return nil, fmt.Errorf("unknown field type %v in: %v", t, f)
	}

	defaultValue, hasDefault := fieldMap["default"]

	var fields []interface{}

	mapFieldsValue, ok := fieldMap["fields"]
	if !ok {
		fields = []interface{}{}
	} else {
		fields, ok = mapFieldsValue.([]interface{})
		if !ok {
			return nil, fmt.Errorf("fields has to be an array: %v", mapFieldsValue)
		}
	}

	var logicalType string
	logicalTypeValue, ok := fieldMap["logicalType"]
	if !ok {
		logicalType = ""
	} else {
		logicalType, ok = logicalTypeValue.(string)
		if !ok {
			return nil, fmt.Errorf("logicaltype has to be a string, but it's current value is: %v", logicalTypeValue)
		}
	}

	return &Field{
		Name:         name,
		Type:         fieldType,
		HasDefault:   hasDefault,
		DefaultValue: defaultValue,
		Fields:       fields,
		LogicalType:  logicalType,
		TypeValue:    typeValue,
	}, nil
}
