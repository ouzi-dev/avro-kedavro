package schema

import (
	"encoding/json"
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
	"github.com/stretchr/testify/assert"
)

//nolint
func TestSchemas(t *testing.T) {
	type testItem struct {
		schema  string
		isError bool
	}
	testSchemas := []testItem{
		{
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
		{
			schema: `
			{
				"name": "curse",
				"type": "string"
			}
			`,
			isError: false,
		},
		{
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
		{
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
		{
			schema: `
			{
				"type": "string",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		{
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
		{
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
		{
			schema: `
			{
				"name": "test",
				"default": "slytherin",
				"logicalType": "test"
			}
			`,
			isError: true,
		},
		{
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
		{
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
		{
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
		{
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

		_, err = ParseSchemaField(asJson, types.Options{})
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestValidUnions(t *testing.T) {
	type testItem struct {
		union        []interface{}
		defaultValue interface{}
		isError      bool
	}
	testSchemas := []testItem{
		{
			union:        []interface{}{"null", "long"},
			isError:      false,
			defaultValue: nil,
		},
		{
			union:        []interface{}{"null", "long"},
			isError:      true,
			defaultValue: "bleh",
		},
		{
			union:        []interface{}{"null", "long"},
			isError:      true,
			defaultValue: 123,
		},
		{
			union:   []interface{}{"null", "long", "string"},
			isError: true,
		},
		{
			union:   []interface{}{"long", "null"},
			isError: true,
		},
		{
			union:   []interface{}{"null", 1234},
			isError: true,
		},
		{
			union:   []interface{}{"null"},
			isError: true,
		},
	}

	for _, v := range testSchemas {
		err := validateUnionFields("test", v.union, v.defaultValue)
		if v.isError {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
