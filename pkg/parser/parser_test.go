package parser

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
)

const testSchema = "../../resources/test.avsc"

const test1 = `
{"curse": "imperious", "house": "slytherin", "wand": "unicorn"}
`

const test2 = `
{"curse": "cruciatus"}
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
