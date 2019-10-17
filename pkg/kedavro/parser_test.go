package kedavro

import (
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
)

//nolint
var spellBytes = []byte("alohomora")

const parserSchema = `
{
	"name": "Voldemort",
	"type": "record",
	"namespace": "com.avro.kedavro",
	"fields": [
	  {
		"name": "name",
		"type": [
		  "null",
		  "string"
		],
		"default": null
	  },
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
		"name": "muggle",
		"type": "null",
		"default": null
	  },
	  {
		"name": "has_broom",
		"type": "boolean",
		"default": true
	  },
	  {
		"name": "spell_bytes",
		"type": "bytes"
	  },
	  {
		"name": "spell_performance",
		"type": "float",
		"default": 50.05
	  },
	  {
		"name": "spell_affinity",
		"type": "double",
		"default": 324235235.5235325
	  },
	  {
		"name": "good_spells",
		"type": "int",
		"default": 0
	  },
	  {
		"name": "evil_spells",
		"type": "long",
		"default": 3
	  },
	  {
		"name": "muggles_killed",
		"type": [
		  "null",
		  "long"
		],
		"default": null
	  },
	  {
		"name": "testing",
		"type": "record",
		"fields": [
		  {
			"name": "name",
			"type": [
			  "null",
			  "string"
			],
			"default": null
		  },
		  {
			"name": "curse",
			"type": "string"
		  }
		]
	  },
	  {
		"name": "metadata",
		"type": {
			"name": "points",
			"type": "record",
			"fields": [
				{
					"name": "house",
					"type": [
					"null",
					"string"
					],
					"default": null
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
`

const test1 = `
{
	"name": "Voldemort",
	"curse": "imperious", 
	"house": "slytherin", 
	"wand": "unicorn", 
	"has_broom": false,
	"spell_bytes":"YWxvaG9tb3Jh",
	"spell_performance": 89.67,
	"spell_affinity": 4.940656458412465441765687928682213723651e-321,
	"bad_spells": 42949672951234,
	"evil_spells": 7,
	"good_spells": 1,
	"muggles_killed": 3,
	"testing": {
		"name": "test",
		"curse":"bleh"
	},
	"metadata": {
		"house": "slytherin",
		"points": 123
	}
}
`

const test2 = `
{
	"name": null,
	"curse": "cruciatus",
	"spell_bytes":"YWxvaG9tb3Jh",
	"spell_performance": 89.67,
	"testing": {
		"curse":"bleh"
	},
	"metadata": {
		"points": 123
	}
}
`

// basically the result of our parser has to be ok for goavro!
func TestParserNoDefaults(t *testing.T) {
	p, err := NewParser(string(parserSchema))
	assert.NoError(t, err)

	result, err := p.Parse([]byte(test1))
	assert.NoError(t, err)

	codec, err := goavro.NewCodec(string(parserSchema))
	assert.NoError(t, err)

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)
}

func TestParserDefaults(t *testing.T) {
	p, err := NewParser(string(parserSchema))
	assert.NoError(t, err)

	result, err := p.Parse([]byte(test2))
	assert.NoError(t, err)

	codec, err := goavro.NewCodec(string(parserSchema))
	assert.NoError(t, err)

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)
}
