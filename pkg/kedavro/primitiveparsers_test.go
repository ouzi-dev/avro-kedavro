package kedavro

import (
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
	"github.com/stretchr/testify/assert"
)

func TestNilPrimitiveType(t *testing.T) {
	fieldNoDefault := getPrimitiveField(nilType, false, nil)
	fieldDefaultValue := getPrimitiveField(nilType, true, nil)
	fieldDefaultWrongValue := getPrimitiveField(nilType, true, "bleh")

	recordWithNullValue := getRecord("test", nil)
	recordWithValueWrongType := getRecord("test", 1234)
	recordWithNoValue := getRecord("bleh", nil)

	tests := []testItem{
		testItem{
			field:    fieldNoDefault,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    fieldNoDefault,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldNoDefault,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithNoValue,
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultWrongValue,
			record:   recordWithNullValue,
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultWrongValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
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

func TestParsePrimitiveFields(t *testing.T) {
	tests := []testType{
		testType{
			fieldType:            stringType,
			validValue:           "testString",
			defaultValue:         "defaultValue",
			wrongValue:           1234,
			expectedValidValue:   "testString",
			expectedDefaultValue: "defaultValue",
		},
		testType{
			fieldType:            boolType,
			validValue:           true,
			defaultValue:         true,
			wrongValue:           1234,
			expectedValidValue:   true,
			expectedDefaultValue: true,
		},
		testType{
			fieldType:            bytesType,
			validValue:           "testString",
			defaultValue:         "defaultValue",
			wrongValue:           1234,
			expectedValidValue:   []byte("testString"),
			expectedDefaultValue: []byte("defaultValue"),
		},
		testType{
			fieldType:            floatType,
			validValue:           float64(12345.67),
			defaultValue:         float64(76543.21),
			wrongValue:           "bleh",
			expectedValidValue:   float32(12345.67),
			expectedDefaultValue: float32(76543.21),
		},
		testType{
			fieldType:            doubleType,
			validValue:           float64(4.94e-321),
			defaultValue:         float64(3.95e-321),
			wrongValue:           "bleh",
			expectedValidValue:   float64(4.94e-321),
			expectedDefaultValue: float64(3.95e-321),
		},
		testType{
			fieldType:            longType,
			validValue:           float64(42949672951234),
			defaultValue:         float64(52949672951234),
			wrongValue:           "bleh",
			expectedValidValue:   int64(42949672951234),
			expectedDefaultValue: int64(52949672951234),
		},
		testType{
			fieldType:            intType,
			validValue:           float64(23456),
			defaultValue:         float64(65432),
			wrongValue:           "bleh",
			expectedValidValue:   int32(23456),
			expectedDefaultValue: int32(65432),
		},
		testType{
			fieldType:            intType,
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
		Type:         schema.Primitive,
		TypeValue:    fieldType,
		Fields:       []interface{}{},
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

func getTestBatch(test testType) []testItem {
	fieldNoDefault := getPrimitiveField(test.fieldType, false, nil)
	fieldDefaultValue := getPrimitiveField(test.fieldType, true, test.defaultValue)
	fieldDefaultWrongValue := getPrimitiveField(test.fieldType, true, test.wrongValue)

	recordWithValue := getRecord("test", test.validValue)
	recordWithValueWrongType := getRecord("test", test.wrongValue)
	recordWithNoValue := getRecord("bleh", test.validValue)
	recordWithNullValue := getRecord("test", nil)

	tests := []testItem{
		testItem{
			field:    fieldNoDefault,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		testItem{
			field:    fieldNoDefault,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldNoDefault,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldNoDefault,
			record:   recordWithNullValue,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithNoValue,
			isError:  false,
			expected: test.expectedDefaultValue,
		},
		testItem{
			field:    fieldDefaultValue,
			record:   recordWithNullValue,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultWrongValue,
			record:   recordWithValue,
			isError:  false,
			expected: test.expectedValidValue,
		},
		testItem{
			field:    fieldDefaultWrongValue,
			record:   recordWithValueWrongType,
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    fieldDefaultWrongValue,
			record:   recordWithNoValue,
			isError:  true,
			expected: nil,
		},
		testItem{
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
		case stringType:
			result, err = parseStringField(v.field, v.record)
		case boolType:
			result, err = parseBoolField(v.field, v.record)
		case bytesType:
			result, err = parseBytesField(v.field, v.record)
		case floatType:
			result, err = parseFloatField(v.field, v.record)
		case doubleType:
			result, err = parseDoubleField(v.field, v.record)
		case longType:
			result, err = parseLongField(v.field, v.record)
		case intType:
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
