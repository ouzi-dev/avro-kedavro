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

// we don't need to test all the different types of union, since we are
// already testing every primitive parser
func TestUnion(t *testing.T) {
	expectedRecordWithString := map[string]interface{}{
		"string": "bleh",
	}

	tests := []testItem{
		testItem{
			field:    getFieldFromJson(unionNoDefault, t),
			record:   getJsonAsNative(jsonWithStringUnion, t),
			isError:  false,
			expected: expectedRecordWithString,
		},
		testItem{
			field:    getFieldFromJson(unionNoDefault, t),
			record:   getJsonAsNative(jsonWithNullUnion, t),
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(unionNoDefault, t),
			record:   getJsonAsNative(jsonWithNumberUnion, t),
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(unionNoDefault, t),
			record:   getJsonAsNative(jsonNoFieldUnion, t),
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(unionDefaultNull, t),
			record:   getJsonAsNative(jsonWithStringUnion, t),
			isError:  false,
			expected: expectedRecordWithString,
		},
		testItem{
			field:    getFieldFromJson(unionDefaultNull, t),
			record:   getJsonAsNative(jsonWithNullUnion, t),
			isError:  false,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(unionDefaultNull, t),
			record:   getJsonAsNative(jsonWithNumberUnion, t),
			isError:  true,
			expected: nil,
		},
		testItem{
			field:    getFieldFromJson(unionDefaultNull, t),
			record:   getJsonAsNative(jsonNoFieldUnion, t),
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
