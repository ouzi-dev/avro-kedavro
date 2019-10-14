package kedavro

import (
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
)

type valueParserFunction func(field *schema.Field, value interface{}) (interface{}, error)

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
		result, err = parsePrimitiveField(field, record)
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

func parsePrimitiveField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
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

func parseStringField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseStringValue)
}

func parseStringValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(string)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"string\"", value, field.Name)
	}

	return v, nil
}

func parseBoolValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(bool)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"boolean\"", value, field.Name)
	}

	return v, nil
}

func parseBoolField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseBoolValue)
}

func parseBytesValue(field *schema.Field, value interface{}) (interface{}, error) {
	// []byte is a string in the json, we need to return it as []byte
	v, ok := value.(string)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"bytes\"", value, field.Name)
	}

	return []byte(v), nil
}

func parseBytesField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseBytesValue)
}

func parseFloatValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"float\"", value, field.Name)
	}

	return float32(v), nil
}

func parseFloatField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseFloatValue)
}

func parseDoubleValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"double\"", value, field.Name)
	}

	return v, nil
}

func parseDoubleField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseDoubleValue)
}

func parseLongValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"long\"", value, field.Name)
	}

	return int64(v), nil
}

func parseLongField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseLongValue)
}

func parseIntValue(field *schema.Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"int\"", value, field.Name)
	}

	return int32(v), nil
}

func parseIntField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseIntValue)
}

func parseNilField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseNilValue)
}

func parseNilValue(field *schema.Field, value interface{}) (interface{}, error) {
	if value != nil {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"null\"", value, field.Name)
	}

	return nil, nil
}

func parseWithDefaultValue(field *schema.Field, record map[string]interface{}, valueParser valueParserFunction) (interface{}, error) {
	value, ok := record[field.Name]
	if !ok {
		if !field.HasDefault {
			return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
		}
		value = field.DefaultValue
	}

	return valueParser(field, value)
}
