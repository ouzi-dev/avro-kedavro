package schema

import (
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
)

type Field struct {
	HasDefault   bool
	Opts         types.Options
	Name         string
	LogicalType  string
	Type         types.FieldType
	TypeValue    interface{}
	DefaultValue interface{}
	Fields       []interface{}
}

func validateUnionFields(name string, unionTypes []interface{}, defaultValue interface{}) error {
	if len(unionTypes) != 2 {
		return fmt.Errorf("only unions with two types are supported, union name \"%s\", types: %v", name, unionTypes)
	}

	if unionTypes[0] != "null" {
		return fmt.Errorf("only unions where the first type is \"null\" are supported, union name \"%s\", types: %v", name, unionTypes)
	}

	if _, ok := unionTypes[1].(string); !ok {
		return fmt.Errorf("only strings are allowed as type in unions, union name \"%s\", types: %v", name, unionTypes)
	}

	if defaultValue != nil {
		return fmt.Errorf("only null is accepted as default value for unions, union name \"%s\", defaultValue: %v", name, defaultValue)
	}

	return nil
}

func ParseSchemaField(f interface{}, opts types.Options) (*Field, error) {
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
	defaultValue, hasDefault := fieldMap["default"]
	var fieldType types.FieldType
	switch t := typeValue.(type) {
	case string:
		fieldType = types.Primitive
	case []interface{}:
		fieldType = types.Union
		// for now we only accept Unions with max two items, and the first one has to be null
		if err := validateUnionFields(name, t, defaultValue); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown field type %v in: %v", t, f)
	}
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
		Opts:         opts,
	}, nil
}
