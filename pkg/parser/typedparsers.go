package parser

import (
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
)

type valueParserFunction func(field *schema.Field, value interface{}) (interface{}, error)

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

func parseNilField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	value, ok := record[field.Name]
	if ok && value != nil {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"null\"", value, field.Name)
	}
	return nil, nil
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

func parseRecordField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	// record is a bit different, first it doesn't use default, and second we just
	// want to check if the object exists to start again processing a new record
	value, ok := record[field.Name]
	if !ok {
		return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
	}

	valueAsMap, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"record\"", value, field.Name)
	}
	return parseRecord(field, valueAsMap)
}
