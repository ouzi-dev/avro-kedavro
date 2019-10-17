package kedavro

import (
	"encoding/json"
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
	"github.com/ouzi-dev/avro-kedavro/pkg/types"
	"github.com/stretchr/testify/assert"
)

const recordEmptyFields = `
{
	"name": "Test",
	"type": "record",
	"fields": [
	]
}
`

const recordNoFields = `
{
	"name": "Test",
	"type": "record"
}
`

const recordWithFields = `
{
	"name": "Test",
	"type": "record",
	"fields": [
		{
			"name": "test",
			"type": "string"
		},
		{
			"name": "test2",
			"type": "string",
			"default": "bleh"
		}
	]
}
`

const jsonWithRecord = `
{
	"Test": {
		"test": "blah"
	}
}
`

const jsonNoRecord = `
{
	"aaaaa": {
		"bbbbb": "cccc"
	}
}
`

const jsonWithRecordDifferentFields = `
{
	"aaaaa": {
		"bbbbb": "cccc"
	}
}
`

func getJSONAsNative(jsonString string, t *testing.T) map[string]interface{} {
	jsonMap := map[string]interface{}{}
	err := json.Unmarshal([]byte(jsonString), &jsonMap)
	assert.NoError(t, err)
	return jsonMap
}

func getFieldFromJSON(jsonString string, t *testing.T) *schema.Field {
	jsonMap := map[string]interface{}{}
	err := json.Unmarshal([]byte(jsonString), &jsonMap)
	assert.NoError(t, err)
	field, err := schema.ParseSchemaField(jsonMap, types.Options{})
	assert.NoError(t, err)
	return field
}

func TestRecordType(t *testing.T) {
	expectedRecordEmpty := map[string]interface{}{}

	expectedRecordWithFields := map[string]interface{}{
		"test":  "blah",
		"test2": "bleh",
	}

	tests := []testItem{
		{
			field:    getFieldFromJSON(recordEmptyFields, t),
			record:   getJSONAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordEmpty,
		},
		{
			field:    getFieldFromJSON(recordNoFields, t),
			record:   getJSONAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordEmpty,
		},
		{
			field:    getFieldFromJSON(recordEmptyFields, t),
			record:   getJSONAsNative(jsonNoRecord, t),
			isError:  true,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(recordNoFields, t),
			record:   getJSONAsNative(jsonNoRecord, t),
			isError:  true,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(recordWithFields, t),
			record:   getJSONAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordWithFields,
		},
		{
			field:    getFieldFromJSON(recordWithFields, t),
			record:   getJSONAsNative(jsonWithRecordDifferentFields, t),
			isError:  true,
			expected: nil,
		},
	}

	for _, v := range tests {
		result, err := parseRecordField(v.field, v.record)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, v.expected, result)
	}
}

func TestRecordWithRecordAsType(t *testing.T) {
	testJSONSchema := `
	{
		"name": "Voldemort",
		"type": {
			"name": "wizard",
			"type": "record",
			"fields": [
				{
					"name": "curse",
					"type": "string"
				},
				{
					"name": "house",
					"type": {
						"name": "test",
						"type": "record",
						"fields": [
							{
								"name": "name",
								"type": "string"
							},
							{
								"name": "points",
								"type": "int" 
							}
						]
					}
				}
			]
		}
	}`

	testJSONRecord := `
	{
		"Voldemort": {
			"curse": "imperio", 
			"house": {
				"name": "slytherin",
				"points": 1234
			}
		}
	}
	`

	expected := map[string]interface{}{
		"curse": "imperio",
		"house": map[string]interface{}{
			"name":   "slytherin",
			"points": int32(1234),
		},
	}

	field := getFieldFromJSON(testJSONSchema, t)

	native := getJSONAsNative(testJSONRecord, t)

	result, err := parseRecordField(field, native)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}
