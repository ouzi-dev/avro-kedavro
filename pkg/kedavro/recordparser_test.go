package kedavro

import (
	"encoding/json"
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
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

func getJsonAsNative(jsonString string, t *testing.T) map[string]interface{} {
	jsonMap := map[string]interface{}{}
	err := json.Unmarshal([]byte(jsonString), &jsonMap)
	assert.NoError(t, err)
	return jsonMap
}

func getFieldFromJson(jsonString string, t *testing.T) *schema.Field {
	jsonMap := map[string]interface{}{}
	err := json.Unmarshal([]byte(jsonString), &jsonMap)
	assert.NoError(t, err)
	field, err := schema.ParseSchemaField(jsonMap)
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
		testItem{
			field:    getFieldFromJson(recordEmptyFields, t),
			record:   getJsonAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordEmpty,
		},
		testItem{
			field:    getFieldFromJson(recordNoFields, t),
			record:   getJsonAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordEmpty,
		},
		testItem{
			field:    getFieldFromJson(recordEmptyFields, t),
			record:   getJsonAsNative(jsonNoRecord, t),
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(recordNoFields, t),
			record:   getJsonAsNative(jsonNoRecord, t),
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(recordWithFields, t),
			record:   getJsonAsNative(jsonWithRecord, t),
			isError:  false,
			expected: expectedRecordWithFields,
		},
		testItem{
			field:    getFieldFromJson(recordWithFields, t),
			record:   getJsonAsNative(jsonWithRecordDifferentFields, t),
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
