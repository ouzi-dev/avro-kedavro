package schema

import (
	"encoding/json"
	"testing"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
	"github.com/stretchr/testify/assert"
)

//nolint
func TestParseFieldRecursive(t *testing.T) {
	opts := types.Options{}
	schema := `
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
		  },
		  {
			"name": "test",
			"type": "record",
			"fields": [
				{
					"name": "son",
					"type": "string"
				},
				{
					"name": "item",
					"type": "string"
				},
				{
					"name": "test2",
					"type": "record",
					"fields": [
						{
							"name": "item2",
							"type": "string"
						}
					]
				}
			]
		  }
		]
	}
	`

	schemaMap := map[string]interface{}{}
	err := json.Unmarshal([]byte(schema), &schemaMap)
	assert.NoError(t, err)

	result, err := ParseSchemaField(schemaMap, opts)
	assert.NoError(t, err)

	assert.Equal(t, "Voldemort", result.Name)
	assert.Equal(t, 4, len(result.Fields))
	assert.Equal(t, "curse", result.Fields[0].Name)
	assert.Equal(t, 0, len(result.Fields[0].Fields))
	assert.Equal(t, "test", result.Fields[3].Name)
	assert.Equal(t, "record", result.Fields[3].TypeValue)
	assert.Equal(t, 3, len(result.Fields[3].Fields))
	assert.Equal(t, "item", result.Fields[3].Fields[1].Name)
	assert.Equal(t, 1, len(result.Fields[3].Fields[2].Fields))
	assert.Equal(t, "item2", result.Fields[3].Fields[2].Fields[0].Name)
}

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
