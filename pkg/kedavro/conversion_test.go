package kedavro

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringToFloat(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "float"
			}
		]
	}
	`

	jsonRecord := `
	{"test": "123.45"}
	`

	expected := map[string]interface{}{
		"test": float32(123.45),
	}

	parser, err := NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": false}
	`

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestStringToDouble(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "double"
			}
		]
	}
	`

	jsonRecord := `
	{"test": "3.95e-321"}
	`

	expected := map[string]interface{}{
		"test": float64(3.95e-321),
	}

	parser, err := NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": true}
	`

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestStringToLong(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "long"
			}
		]
	}
	`

	jsonRecord := `
	{"test": "52949672951234"}
	`

	expected := map[string]interface{}{
		"test": int64(52949672951234),
	}

	parser, err := NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": false}
	`

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestStringToInt(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "int"
			}
		]
	}
	`

	jsonRecord := `
	{"test": "63554737"}
	`

	expected := map[string]interface{}{
		"test": int32(63554737),
	}

	parser, err := NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": true}
	`

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

//nolint
func TestStringToBool(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "boolean"
			}
		]
	}
	`

	jsonRecord := `
	{"test": "TRUE"}
	`

	expected := map[string]interface{}{
		"test": true,
	}

	parser, err := NewParser(schema, WithStringToBool())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": "   TrUe  "}
	`

	expected = map[string]interface{}{
		"test": true,
	}

	parser, err = NewParser(schema, WithStringToBool())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": "   fAlSe  "}
	`

	expected = map[string]interface{}{
		"test": false,
	}

	parser, err = NewParser(schema, WithStringToBool())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
	{"test": 1234}
	`

	parser, err = NewParser(schema, WithStringToBool())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

//nolint
func TestTimestampToMillis(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "long",
				"logicalType": "timestamp-millis"
			}
		]
	}
	`

	jsonRecord := `
	{"test": 1571057118}
	`

	expected := map[string]interface{}{
		"test": int64(1571057118000),
	}

	parser, err := NewParser(schema, WithTimestampToMillis())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": "1571057118"}
		`

	expected = map[string]interface{}{
		"test": int64(1571057118000),
	}

	parser, err = NewParser(schema, WithTimestampToMillis(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": 1571057118}
		`

	expected = map[string]interface{}{
		"test": int64(1571057118),
	}

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": "aa1571057118"}
		`

	parser, err = NewParser(schema, WithTimestampToMillis(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

//nolint
func TestTimestampToMicros(t *testing.T) {

	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": "long",
				"logicalType": "timestamp-micros"
			}
		]
	}
	`

	jsonRecord := `
	{"test": 1571057118}
	`

	expected := map[string]interface{}{
		"test": int64(1571057118),
	}

	parser, err := NewParser(schema)
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": 1571057118}
		`

	expected = map[string]interface{}{
		"test": int64(1571057118000000),
	}

	parser, err = NewParser(schema, WithTimestampToMicros())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": "1571057118"}
		`

	expected = map[string]interface{}{
		"test": int64(1571057118000000),
	}

	parser, err = NewParser(schema, WithTimestampToMicros(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	jsonRecord = `
		{"test": "aa1571057118"}
		`

	parser, err = NewParser(schema, WithTimestampToMicros(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}
