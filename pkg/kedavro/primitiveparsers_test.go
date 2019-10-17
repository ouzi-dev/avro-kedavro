package kedavro

import (
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
	"github.com/ouzi-dev/avro-kedavro/pkg/types"
	"github.com/stretchr/testify/assert"
)

//nolint
func TestNilPrimitiveType(t *testing.T) {
	fieldNoDefault := getPrimitiveField(types.NilType, false, nil)
	fieldDefaultValue := getPrimitiveField(types.NilType, true, nil)
	fieldDefaultWrongValue := getPrimitiveField(types.NilType, true, "bleh")

	recordWithNullValue := getRecord("test", nil)
	recordWithValueWrongType := getRecord("test", 1234)
	recordWithNoValue := getRecord("bleh", nil)

	tests := []testItem{
		{
			field:    fieldNoDefault,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		{
			field:    fieldNoDefault,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldNoDefault,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithNoValue,
			isError:  false,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
	}

	for _, v := range tests {
		result, err := parseNilField(v.field, v.record)
		assert.Equal(t, v.expected, result)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

//nolint
func TestParsePrimitiveFields(t *testing.T) {
	tests := []testType{
		{
			fieldType:            types.StringType,
			validValue:           "testString",
			defaultValue:         "defaultValue",
			wrongValue:           1234,
			expectedValidValue:   "testString",
			expectedDefaultValue: "defaultValue",
		},
		{
			fieldType:            types.BoolType,
			validValue:           true,
			defaultValue:         true,
			wrongValue:           1234,
			expectedValidValue:   true,
			expectedDefaultValue: true,
		},
		{
			fieldType:            types.BytesType,
			validValue:           "testString",
			defaultValue:         "defaultValue",
			wrongValue:           1234,
			expectedValidValue:   []byte("testString"),
			expectedDefaultValue: []byte("defaultValue"),
		},
		{
			fieldType:            types.FloatType,
			validValue:           float64(12345.67),
			defaultValue:         float64(76543.21),
			wrongValue:           "bleh",
			expectedValidValue:   float32(12345.67),
			expectedDefaultValue: float32(76543.21),
		},
		{
			fieldType:            types.DoubleType,
			validValue:           float64(4.94e-321),
			defaultValue:         float64(3.95e-321),
			wrongValue:           "bleh",
			expectedValidValue:   float64(4.94e-321),
			expectedDefaultValue: float64(3.95e-321),
		},
		{
			fieldType:            types.LongType,
			validValue:           float64(42949672951234),
			defaultValue:         float64(52949672951234),
			wrongValue:           "bleh",
			expectedValidValue:   int64(42949672951234),
			expectedDefaultValue: int64(52949672951234),
		},
		{
			fieldType:            types.IntType,
			validValue:           float64(23456),
			defaultValue:         float64(65432),
			wrongValue:           "bleh",
			expectedValidValue:   int32(23456),
			expectedDefaultValue: int32(65432),
		},
		{
			fieldType:            types.IntType,
			validValue:           float64(23456),
			defaultValue:         float64(65432),
			wrongValue:           "bleh",
			expectedValidValue:   int32(23456),
			expectedDefaultValue: int32(65432),
		},
	}

	for _, v := range tests {
		testPrimitiveField(t, v)
	}

}

func getPrimitiveField(fieldType string, hasDefault bool, defaultValue interface{}) *schema.Field {
	return &schema.Field{
		Name:         "test",
		Type:         types.Primitive,
		TypeValue:    fieldType,
		Fields:       []*schema.Field{},
		HasDefault:   hasDefault,
		DefaultValue: defaultValue,
	}
}

func getRecord(fieldName string, value interface{}) map[string]interface{} {
	record := map[string]interface{}{
		fieldName: value,
	}
	return record
}

type testItem struct {
	field    *schema.Field
	record   map[string]interface{}
	isError  bool
	expected interface{}
}

//nolint
func getTestBatch(test testType) []testItem {
	fieldNoDefault := getPrimitiveField(test.fieldType, false, nil)
	fieldDefaultValue := getPrimitiveField(test.fieldType, true, test.defaultValue)
	fieldDefaultWrongValue := getPrimitiveField(test.fieldType, true, test.wrongValue)

	recordWithValue := getRecord("test", test.validValue)
	recordWithValueWrongType := getRecord("test", test.wrongValue)
	recordWithNoValue := getRecord("bleh", test.validValue)
	recordWithNullValue := getRecord("test", nil)

	tests := []testItem{
		{
			field:    fieldNoDefault,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		{
			field:    fieldNoDefault,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldNoDefault,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldNoDefault,
			record:   recordWithNullValue,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithNoValue,
			isError:  false,
			expected: test.expectedDefaultValue,
		},
		{
			field:    fieldDefaultValue,
			record:   recordWithNullValue,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		{
			field:    fieldDefaultWrongValue,
			record:   recordWithNullValue,
			isError:  true,
			expected: nil,
		},
	}

	return tests
}

type testType struct {
	fieldType            string
	validValue           interface{}
	defaultValue         interface{}
	wrongValue           interface{}
	expectedValidValue   interface{}
	expectedDefaultValue interface{}
}

func testPrimitiveField(t *testing.T, test testType) {
	tests := getTestBatch(test)

	for _, v := range tests {
		var result interface{}
		var err error

		switch test.fieldType {
		case types.StringType:
			result, err = parseStringField(v.field, v.record)
		case types.BoolType:
			result, err = parseBoolField(v.field, v.record)
		case types.BytesType:
			result, err = parseBytesField(v.field, v.record)
		case types.FloatType:
			result, err = parseFloatField(v.field, v.record)
		case types.DoubleType:
			result, err = parseDoubleField(v.field, v.record)
		case types.LongType:
			result, err = parseLongField(v.field, v.record)
		case types.IntType:
			result, err = parseIntField(v.field, v.record)
		default:
			assert.Fail(t, "unknown primitive field "+test.fieldType)
		}

		assert.Equal(t, v.expected, result)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
