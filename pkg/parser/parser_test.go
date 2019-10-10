package parser

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
)

const testSchema = "../../resources/test.avsc"

var spellBytes = []byte("alohomora")

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
	}
}
`

func TestParser(t *testing.T) {
	schema, err := ioutil.ReadFile(testSchema)
	assert.NoError(t, err)

	p, err := NewParser(string(schema[:]))
	assert.NoError(t, err)

	result, err := p.Parse([]byte(test1))
	assert.NoError(t, err)

	codec, err := goavro.NewCodec(string(schema))
	assert.NoError(t, err)

	textual, err := codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	fmt.Println(string(textual[:]))

	result, err = p.Parse([]byte(test2))
	assert.NoError(t, err)

	codec, err = goavro.NewCodec(string(schema))
	assert.NoError(t, err)

	textual, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	fmt.Println(string(textual[:]))
}
