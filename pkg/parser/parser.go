package parser

import (
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro"
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

	avroRecord := map[string]interface{}{}

	for _, v := range p.schema.Fields {
		field, err := schema.ParseSchemaField(v)

		if err != nil {
			return nil, err
		}

		newField, err := p.parseField(field, jsonRecord)
		if err != nil {
			return nil, fmt.Errorf("field parse error, field: %v, error: %v", field, err)
		}

		avroRecord[field.Name] = newField
	}

	return avroRecord, nil
}

func (p *parser) parseField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch field.Type {
	case schema.Primitive:
		result, err = p.parseTypedField(field, record)
	case schema.Union:
		// Union
		result, err = p.parseUnionField(field, record)
	default:
		err = fmt.Errorf("unknown field type in field %s", field.Name)
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *parser) parseUnionField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	value, ok := record[field.Name]
	if !ok {
		if !field.HasDefault {
			return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
		} else {
			typeName, err := p.GetStringType(field.DefaultValue)
			if err != nil {
				return nil, err
			}
			return goavro.Union(typeName, field.DefaultValue), nil
		}
	}

	typeName, err := p.GetStringType(value)
	if err != nil {
		return nil, err
	}

	return goavro.Union(typeName, value), nil
}

func (p *parser) parseTypedField(
	field *schema.Field,
	record map[string]interface{},
) (interface{}, error) {

	value, ok := record[field.Name]
	if !ok {
		if !field.HasDefault {
			return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
		} else {
			return field.DefaultValue, nil
		}
	}

	var parsedValue interface{}
	var err error

	fieldType := field.TypeValue.(string)

	switch fieldType {
	case stringType:
		parsedValue, err = parseStringField(field.Name, value)
	case nilType:
	case boolType:
	case bytesType:
	case floatType:
	case doubleType:
	case longType:
	case intType:
	case arrayType:
	default:
		return nil, fmt.Errorf("type \"%s\" not supported yet...", fieldType)
	}

	return parsedValue, err
}

func (p *parser) GetStringType(t interface{}) (string, error) {
	switch v := t.(type) {
	case nil:
		return nilType, nil
	case bool:
		return boolType, nil
	case []byte:
		return bytesType, nil
	case float32:
		return floatType, nil
	case float64:
		return doubleType, nil
	case int64:
		return longType, nil
	case int32:
		return intType, nil
	case string:
		return stringType, nil
	case []interface{}:
		return arrayType, nil
	default:
		return "", fmt.Errorf("unknow type %v for \"%v\"", v, t)
	}
}

/*

double	float64
long	int64
int	    int32
string	string
array	[]interface{}
enum	string
fixed	[]byte
map and record	map[string]interface{}
*/
