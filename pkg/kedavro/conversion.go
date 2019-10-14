package kedavro

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
)

func getStringAs(value interface{}, returnType string) (interface{}, error) {
	s, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("value \"%v\" is not a string", value)
	}

	var parsedValue interface{}
	var err error
	switch returnType {
	case types.BoolType:
		parsedValue, err = stringToBool(s)
	case types.FloatType:
		parsedValue, err = stringToFloat(s)
	case types.DoubleType:
		parsedValue, err = stringToDouble(s)
	case types.LongType:
		parsedValue, err = stringToLong(s)
	case types.IntType:
		parsedValue, err = stringToInt(s)
	default:
		return nil, fmt.Errorf("string to \"%s\" not supported", returnType)
	}

	return parsedValue, err
}

func stringToInt(value string) (interface{}, error) {
	s, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("string \"%s\" not valid as int", value)
	}
	return int32(s), nil
}

func stringToLong(value string) (interface{}, error) {
	s, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("string \"%s\" not valid as long", value)
	}
	return s, nil
}

func stringToDouble(value string) (interface{}, error) {
	s, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("string \"%s\" not valid as double", value)
	}
	return s, nil
}

func stringToFloat(value string) (interface{}, error) {
	s, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return nil, fmt.Errorf("string \"%s\" not valid as float", value)
	}
	return float32(s), nil
}

func stringToBool(value string) (interface{}, error) {
	formattedValue := strings.ToLower(strings.TrimSpace(value))
	switch formattedValue {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return nil, fmt.Errorf("string \"%s\" not valid as boolean", value)
	}
}
