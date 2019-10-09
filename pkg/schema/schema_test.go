package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchemas(t *testing.T) {
	type testItem struct {
		schema  string
		isError bool
	}
	testSchemas := []testItem{
		testItem{
			schema: `
			{
				"name": "Voldemort",
				"type": "record",
				"fields": [
				  {
					"name": "curse",
					"type": "string"
				  },
				  {
					"name": "house",
					"type": "string",
					"default": "slytherin"
				  },
				  {
					"name": "wand",
					"type": [
					  "null",
					  "string"
					],
					"default": null
				  }
				]
			}
			`,
			isError: false,
		},
		testItem{
			schema: `
			{
				"name": "curse",
				"type": "string"
			}
			`,
			isError: false,
		},
		testItem{
			schema: `
			{
				"name": "wand",
				"type": [
				  "null",
				  "string"
				],
				"default": null
			}
			`,
			isError: false,
		},
		testItem{
			schema: `
			{
				"name": "house",
				"type": "string",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: false,
		},
		testItem{
			schema: `
			{
				"type": "string",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "",
				"type": "string",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": 1234,
				"type": "string",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "test",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "test",
				"type": 1324,
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "test",
				"type": {"test":"test"},
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "test",
				"type": "string",
				"default": "slytherin",
				"logicalType": 1233
			}
			`,
			isError: true,
		},
		testItem{
			schema: `
			{
				"name": "test",
				"type": "string",
				"default": "slytherin",
				"fields": "test"
			}
			`,
			isError: true,
		},
	}

	for _, v := range testSchemas {
		asJson := map[string]interface{}{}
		err := json.Unmarshal([]byte(v.schema), &asJson)
		assert.NoError(t, err)

		_, err = ParseSchemaField(asJson)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
