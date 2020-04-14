package kedavro

import (
	"reflect"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
)

const failJSONRecord = `
{"test": false}
`

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

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(failJSONRecord))
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestUnionStringToLong(t *testing.T) {
	schema := `
	{
		"name": "Test",
		"type": "record",
		"fields": [
			{
				"name": "test",
				"type": [
					"null",
					"long"
				],
				"default": null
			}
		]
	}
	`

	jsonRecord := `
	{"test": "123"}
	`

	expected := map[string]interface{}{
		"test": map[string]interface{}{
			"long": int64(123),
		},
	}

	parser, err := NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(failJSONRecord))
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

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(failJSONRecord))
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
func TestDateStringToTimestamp(t *testing.T) {
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
	}`

	codec, err := goavro.NewCodec(schema)
	assert.NoError(t, err)

	jsonRecord := `
	{"test": "2019-10-14T12:45:18Z"}
	`

	expected := map[string]interface{}{
		"test": time.Unix(1571057118, 0).UTC(),
	}

	parser, err := NewParser(schema, WithDateTimeFormat(time.RFC3339))
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
	{"test": "12:45:18-2019-10-14"}
	`

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

	codec, err := goavro.NewCodec(schema)
	assert.NoError(t, err)

	jsonRecord := `
	{"test": 1571057118}
	`

	expected := map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err := NewParser(schema, WithTimestampToMillis())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if number with decimals provided it ignores the decimals
	jsonRecord = `
	{"test": 1571057118.4566788943}
	`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err = NewParser(schema, WithTimestampToMillis())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
		{"test": "1571057118"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err = NewParser(schema, WithTimestampToMillis(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if string with number with decimals provided it keeps decimals as milliseconds
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(1571057118, 123*int64(time.Millisecond)),
	}

	parser, err = NewParser(schema, WithTimestampToMillis(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if string with number with decimals provided it keeps decimals as milliseconds
	// even without conversion to millis
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(1571057118, 123*int64(time.Millisecond)),
	}

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)

	// timestamp with millis and no conversion
	jsonRecord = `
		{"test": 1571057118123}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118123*int64(time.Millisecond)),
	}

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// timestamp with millis and conversion from string
	jsonRecord = `
		{"test": "1571057118123"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118123*int64(time.Millisecond)),
	}

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
		{"test": "aa1571057118"}
		`

	parser, err = NewParser(schema, WithTimestampToMillis(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)

	// if string is number with decimals and no WithStringToNumber it fails
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)

	jsonRecord = `
		{"bleh": "blah"}
		`

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)

	jsonRecord = `
		{"bleh": "blah"}
		`

	parser, err = NewParser(schema, WithNowForNullTimestamp())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)

	resultAsMap, ok := result.(map[string]interface{})
	assert.True(t, ok)

	assert.NotNil(t, resultAsMap["test"])

	resultTime := resultAsMap["test"].(time.Time)

	now := time.Now()
	// just checking the returned time is between now and 2 seconds ago...
	// if the test took more than 2 seconds... it deserves to fail :D
	assert.True(t, now.Before(resultTime.Add(2*time.Second)))
	assert.True(t, resultTime.Before(now))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
	{"test": null}
	`

	parser, err = NewParser(schema, WithNowForNullTimestamp())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)

	resultAsMap, ok = result.(map[string]interface{})
	assert.True(t, ok)

	assert.NotNil(t, resultAsMap["test"])

	resultTime = resultAsMap["test"].(time.Time)

	now = time.Now()
	// just checking the returned time is between now and 2 seconds ago...
	// if the test took more than 2 seconds... it deserves to fail :D
	assert.True(t, now.Before(resultTime.Add(2*time.Second)))
	assert.True(t, resultTime.Before(now))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)
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

	codec, err := goavro.NewCodec(schema)
	assert.NoError(t, err)

	jsonRecord := `
	{"test": 1571057118}
	`

	expected := map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err := NewParser(schema, WithTimestampToMicros())
	assert.NoError(t, err)

	result, err := parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if number with decimals provided it ignores the decimals
	jsonRecord = `
	{"test": 1571057118.4566788943}
	`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err = NewParser(schema, WithTimestampToMicros())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
		{"test": "1571057118"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118*int64(time.Second)),
	}

	parser, err = NewParser(schema, WithTimestampToMicros(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if string with number with decimals provided it keeps decimals as microseconds
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(1571057118, 123456*int64(time.Microsecond)),
	}

	parser, err = NewParser(schema, WithTimestampToMicros(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// if string with number with decimals provided it keeps decimals as milliseconds
	// even without conversion to micros
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(1571057118, 123456*int64(time.Microsecond)),
	}

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)

	// timestamp with microseconds and no conversion
	jsonRecord = `
		{"test": 1571057118123456}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118123456*int64(time.Microsecond)),
	}

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	// timestamp with microseconds and conversion from string
	jsonRecord = `
		{"test": "1571057118123456"}
		`

	expected = map[string]interface{}{
		"test": time.Unix(0, 1571057118123456*int64(time.Microsecond)),
	}

	parser, err = NewParser(schema, WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(expected, result))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
		{"test": "aa1571057118"}
		`

	parser, err = NewParser(schema, WithTimestampToMicros(), WithStringToNumber())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)

	// if string is number with decimals and no WithStringToNumber it fails
	jsonRecord = `
		{"test": "1571057118.12345678"}
		`

	parser, err = NewParser(schema)
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.Error(t, err)
	assert.Nil(t, result)

	jsonRecord = `
		{"bleh": "blah"}
		`

	parser, err = NewParser(schema, WithNowForNullTimestamp())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)

	resultAsMap, ok := result.(map[string]interface{})
	assert.True(t, ok)

	assert.NotNil(t, resultAsMap["test"])

	resultTime := resultAsMap["test"].(time.Time)

	now := time.Now()
	// just checking the returned time is between now and 2 seconds ago...
	// if the test took more than 2 seconds... it deserves to fail :D
	assert.True(t, now.Before(resultTime.Add(2*time.Second)))
	assert.True(t, resultTime.Before(now))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)

	jsonRecord = `
	{"test": null}
	`

	parser, err = NewParser(schema, WithNowForNullTimestamp())
	assert.NoError(t, err)

	result, err = parser.Parse([]byte(jsonRecord))
	assert.NoError(t, err)

	resultAsMap, ok = result.(map[string]interface{})
	assert.True(t, ok)

	assert.NotNil(t, resultAsMap["test"])

	resultTime = resultAsMap["test"].(time.Time)

	now = time.Now()
	// just checking the returned time is between now and 2 seconds ago...
	// if the test took more than 2 seconds... it deserves to fail :D
	assert.True(t, now.Before(resultTime.Add(2*time.Second)))
	assert.True(t, resultTime.Before(now))

	_, err = codec.TextualFromNative(nil, result)
	assert.NoError(t, err)
}
