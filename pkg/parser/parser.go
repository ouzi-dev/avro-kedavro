package parser

import (
	"encoding/json"
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
)

type parser struct {
	schema *schema.Field
}

type Parser interface {
	Parse(record []byte) (interface{}, error)
}

func NewParser(schemaString string) (Parser, error) {
	s := map[string]interface{}{}

	if err := json.Unmarshal([]byte(schemaString), &s); err != nil {
		return nil, fmt.Errorf("unmarshall schema failed: %v", err)
	}

	rootField, err := schema.ParseSchemaField(s)
	if err != nil {
		return nil, err
	}

	if rootField.Type != schema.Primitive || rootField.TypeValue.(string) != "record" {
		return nil, fmt.Errorf("schema root field must be of type record")
	}

	return &parser{
		schema: rootField,
	}, nil
}

func (p *parser) Parse(record []byte) (interface{}, error) {
	jsonRecord := map[string]interface{}{}
	if err := json.Unmarshal(record, &jsonRecord); err != nil {
		return nil, fmt.Errorf("unmarshall record failed: %v", err)
	}

	return parseRecord(p.schema, jsonRecord)
}

func parseRecord(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	avroRecord := map[string]interface{}{}

	for _, v := range field.Fields {
		field, err := schema.ParseSchemaField(v)

		if err != nil {
			return nil, err
		}

		newField, err := parseField(field, record)
		if err != nil {
			return nil, fmt.Errorf("field parse error, field: %v, error: %v", field, err)
		}

		avroRecord[field.Name] = newField
	}

	return avroRecord, nil
}

func parseField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch field.Type {
	case schema.Primitive:
		result, err = parseTypedField(field, record)
	case schema.Union:
		// Union
		result, err = parseUnionField(field, record)
	default:
		err = fmt.Errorf("unknown field type in field %s", field.Name)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseTypedField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	var parsedValue interface{}
	var err error

	fieldType := field.TypeValue.(string)

	switch fieldType {
	case stringType:
		parsedValue, err = parseStringField(field, record)
	case nilType:
		parsedValue, err = parseNilField(field, record)
	case boolType:
		parsedValue, err = parseBoolField(field, record)
	case bytesType:
		parsedValue, err = parseBytesField(field, record)
	case floatType:
		parsedValue, err = parseFloatField(field, record)
	case doubleType:
		parsedValue, err = parseDoubleField(field, record)
	case longType:
		parsedValue, err = parseLongField(field, record)
	case intType:
		parsedValue, err = parseIntField(field, record)
	case recordType:
		parsedValue, err = parseRecordField(field, record)
	default:
		return nil, fmt.Errorf("type \"%s\" not supported yet...", fieldType)
	}

	return parsedValue, err
}
