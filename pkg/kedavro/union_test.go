package kedavro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const unionNoDefault = `
{
	"name": "test",
	"type": [
	  "null",
	  "string"
	]
}
`

const unionDefaultNull = `
{
	"name": "test",
	"type": [
	  "null",
	  "string"
	],
	"default": null
}
`

const jsonWithNullUnion = `
{"test":null}
`

const jsonWithStringUnion = `
{"test": "bleh"}
`

const jsonWithNumberUnion = `
{"test": 1234}
`

const jsonNoFieldUnion = `
{"blah": "blah"}
`

//nolint
// we don't need to test all the different types of union, since we are
// already testing every primitive parser
func TestUnion(t *testing.T) {
	expectedRecordWithString := map[string]interface{}{
		"string": "bleh",
	}

	tests := []testItem{
		{
			field:    getFieldFromJSON(unionNoDefault, t),
			record:   getJSONAsNative(jsonWithStringUnion, t),
			isError:  false,
			expected: expectedRecordWithString,
		},
		{
			field:    getFieldFromJSON(unionNoDefault, t),
			record:   getJSONAsNative(jsonWithNullUnion, t),
			isError:  false,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(unionNoDefault, t),
			record:   getJSONAsNative(jsonWithNumberUnion, t),
			isError:  true,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(unionNoDefault, t),
			record:   getJSONAsNative(jsonNoFieldUnion, t),
			isError:  true,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(unionDefaultNull, t),
			record:   getJSONAsNative(jsonWithStringUnion, t),
			isError:  false,
			expected: expectedRecordWithString,
		},
		{
			field:    getFieldFromJSON(unionDefaultNull, t),
			record:   getJSONAsNative(jsonWithNullUnion, t),
			isError:  false,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(unionDefaultNull, t),
			record:   getJSONAsNative(jsonWithNumberUnion, t),
			isError:  true,
			expected: nil,
		},
		{
			field:    getFieldFromJSON(unionDefaultNull, t),
			record:   getJSONAsNative(jsonNoFieldUnion, t),
			isError:  false,
			expected: nil,
		},
	}

	for _, v := range tests {
		result, err := parseUnionField(v.field, v.record)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, v.expected, result)
	}
}
