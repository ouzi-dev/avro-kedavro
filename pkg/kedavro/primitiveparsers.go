package kedavro

import (
	"fmt"
	"math"
	"time"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
)

type valueParserFunction func(field *Field, value interface{}) (interface{}, error)

func parseRecord(field *Field, record map[string]interface{}) (interface{}, error) {
	avroRecord := map[string]interface{}{}

	for _, v := range field.Fields {
		newField, err := parseField(v, record)
		if err != nil {
			return nil, fmt.Errorf("field parse error, field: %v, error: %v", v, err)
		}

		avroRecord[v.Name] = newField
	}

	return avroRecord, nil
}

func parseField(field *Field, record map[string]interface{}) (interface{}, error) {
	var result interface{}
	var err error
	switch field.Type {
	case types.Primitive:
		result, err = parsePrimitiveField(field, record)
	case types.Union:
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

func parsePrimitiveField(field *Field, record map[string]interface{}) (interface{}, error) {
	return field.ParseField(field, record)
}

func parseStringField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseStringValue)
}

func parseStringValue(field *Field, value interface{}) (interface{}, error) {
	v, ok := value.(string)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"string\"", value, field.Name)
	}

	return v, nil
}

func parseBoolValue(field *Field, value interface{}) (interface{}, error) {
	v, ok := value.(bool)

	if !ok {
		if field.Opts.IsStringToBool {
			f, err := getStringAs(value, types.BoolType)
			if err != nil {
				return nil, fmt.Errorf("parsing string in field \"%s\" error: %v", field.Name, err)
			}
			return f, nil
		}
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"boolean\"", value, field.Name)
	}

	return v, nil
}

func parseBoolField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseBoolValue)
}

func parseBytesValue(field *Field, value interface{}) (interface{}, error) {
	// []byte is a string in the json, we need to return it as []byte
	v, ok := value.(string)

	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"bytes\"", value, field.Name)
	}

	return []byte(v), nil
}

func parseBytesField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseBytesValue)
}

func parseFloatValue(field *Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		if field.Opts.IsStringToNumber {
			f, err := getStringAs(value, types.FloatType)
			if err != nil {
				return nil, fmt.Errorf("parsing string in field \"%s\" error: %v", field.Name, err)
			}
			return f, nil
		}
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"float\"", value, field.Name)
	}

	return float32(v), nil
}

func parseFloatField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseFloatValue)
}

func parseDoubleValue(field *Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		if field.Opts.IsStringToNumber {
			f, err := getStringAs(value, types.DoubleType)
			if err != nil {
				return nil, fmt.Errorf("parsing string in field \"%s\" error: %v", field.Name, err)
			}
			return f, nil
		}
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"double\"", value, field.Name)
	}

	return v, nil
}

func parseDoubleField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseDoubleValue)
}

func parseLongValueAsNumber(field *Field, value interface{}) (interface{}, error) {
	var result int64

	v, ok := value.(float64)

	if !ok {
		if field.Opts.IsStringToNumber {
			f, err := getStringAs(value, types.LongType)
			if err != nil {
				return nil, fmt.Errorf("parsing string in field \"%s\" error: %v", field.Name, err)
			}
			result = f.(int64)
		} else {
			return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"long\"", value, field.Name)
		}
	} else {
		result = int64(v)
	}

	return result, nil
}

func parseLongValueAsTimestamp(field *Field, value interface{}) (interface{}, error) {
	// so timestamp is a bit different... we need to try first, and if we get an error we need to check if we need to format the date
	v, err := parseLongValueAsNumber(field, value)

	if err != nil {
		// special case, if we get a timestamp as number with decimals but it's a string...
		// it will fail parsing to long, but we can deal with it as a double
		d, err := parseDoubleValue(field, value)
		if err == nil {
			asFloat := d.(float64)
			sec, dec := math.Modf(asFloat)

			var factor float64
			//now we need to keep millisecs or microsecs
			if field.LogicalType == types.TimestampMillis {
				factor = 1000
			} else {
				factor = 1000000
			}

			f := int64(dec * factor)

			if field.LogicalType == types.TimestampMillis {
				return time.Unix(int64(sec), f*int64(time.Millisecond)), nil
			}

			return time.Unix(int64(sec), f*int64(time.Microsecond)), nil
		}

		// no we couldn't parse it as a long or a double so let's check if it's a string
		// with the format passed as parameter
		if field.Opts.IsFormatDateTime {
			f, ok := value.(string)
			if !ok {
				// we can't parse as number and it's not a string so... error
				return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"long\", \"double\" or \"string\"", value, field.Name)
			}
			t, err := time.Parse(field.Opts.DateTimeFormat, f)
			if err != nil {
				return nil, fmt.Errorf("error while parsing value \"%v\" in field \"%s\" as date with format \"%s\"", value, field.Name, field.Opts.DateTimeFormat)
			}
			return t, nil
		}
		return nil, err
	}

	result := v.(int64)

	// now we have to parse the long to a time.Time, if we have any of the flags on it's easy
	if field.Opts.IsTimestampToMillis || field.Opts.IsTimestampToMicros {
		return time.Unix(result, 0), nil
	}

	if field.LogicalType == types.TimestampMillis {
		return time.Unix(0, result*int64(time.Millisecond)), nil
	}
	return time.Unix(0, result*int64(time.Microsecond)), nil
}

func parseLongValue(field *Field, value interface{}) (interface{}, error) {
	if field.LogicalType == types.TimestampMillis || field.LogicalType == types.TimestampMicros {
		return parseLongValueAsTimestamp(field, value)
	}
	return parseLongValueAsNumber(field, value)
}

func parseLongField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseLongValue)
}

func parseIntValue(field *Field, value interface{}) (interface{}, error) {
	v, ok := value.(float64)

	if !ok {
		if field.Opts.IsStringToNumber {
			f, err := getStringAs(value, types.IntType)
			if err != nil {
				return nil, fmt.Errorf("parsing string in field \"%s\" error: %v", field.Name, err)
			}
			return f, nil
		}
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"int\"", value, field.Name)
	}

	return int32(v), nil
}

func parseIntField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseIntValue)
}

func parseNilField(field *Field, record map[string]interface{}) (interface{}, error) {
	return parseWithDefaultValue(field, record, parseNilValue)
}

func parseNilValue(field *Field, value interface{}) (interface{}, error) {
	if value != nil {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"null\"", value, field.Name)
	}

	return nil, nil
}

func parseWithDefaultValue(field *Field, record map[string]interface{}, valueParser valueParserFunction) (interface{}, error) {
	value, ok := record[field.Name]
	if !ok {
		if !field.HasDefault {
			return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
		}
		value = field.DefaultValue
	}

	return valueParser(field, value)
}
