# avro-kedavro

A library to parse raw json to avro with magic!

## Why avro-kedavro?

* We want to store information about wizards in S3 to query with Athena later using this schema:

```
{
  "name": "Wizard",
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
		"name": "id",
		"type": "long"
	  },
      {
		"name": "timestamp",
		"type": "long",
        "logicalType": "timestamp-millis"
	  }
  ]
}
```

* We have multiple sources reporting wizards and leaving the data in JSON format in a stream... the problem is that each source uses a different format, so we have reports like:

    * `{"name": "Harry", "id": 12345, "timestamp": 1571128870}`
    * `{"name": "Ron", "id": "98765", "timestamp": 1571128870}`
    * `{"name": "Hermione", "id": "56784", "timestamp": "1571128870000"}`

    None of these reports are valid for the schema we have:

    * All of them will fail just with the `name` field, since the union in JSON-avro should be: `"name": {"string": "..."}`
    * Only the first record has the id as `long`
    * The  schema expects the timestamp as a `long` with milliseconds, but none of the reports is correct: or they don't have milliseconds, or it's a `string` instead of a `long`
    
* We could try to implement an specific solution for each record, but what happens when we start dealing with 10 different types? And with 100? And even more, what if we want to change some schemas? Changing a schema would mean to go through all the parsers we built for specific "events". So we need some kind of magic where we have:
    * avro schema
    * JSON record
    * Some rules like: switch strings to numbers, or switch timestamps to timestamps with milliseconds, ...

    Well... that magic is `avro-kedavro`!

### How to use it

`avro-kedavro` is design to work with [goavro](https://github.com/linkedin/goavro). The idea is `avro-kedavro` will parse your raw JSON to avro-JSON supported by your schema, so you can use [goavro](https://github.com/linkedin/goavro) to generate your [avro OCF files](https://avro.apache.org/docs/1.8.1/spec.html#Object+Container+Files)

Example:

```
import (
	"encoding/json"
	"fmt"

	"github.com/linkedin/goavro"
	"github.com/ouzi-dev/avro-kedavro/pkg/kedavro"
)

const schema = `{
	"name": "Wizard",
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
		  "name": "id",
		  "type": "long"
		},
		{
		  "name": "timestamp",
		  "type": "long",
		  "logicalType": "timestamp-millis"
		}
	]
  }`

const JSONrecord = `{"name": "Voldemort", "id": "66666", "timestamp": "1571128870"}`

func ParseToJSONAvro() error {
	p, err := kedavro.NewParser(schema, kedavro.WithStringToNumber(), kedavro.WithTimestampToMillis())
	if err != nil {
		// Error parsing schema
		return err
	}

	avroJSON, err := p.Parse([]byte(JSONrecord))
	if err != nil {
		// Error parsing record with schema
		return err
	}

	// Marshal the map to show the result from avro-kedavro
	kedavroJSONResult, err := json.Marshal(avroJSON)
	if err != nil {
		// Error marshaling kedavro result
		return err
	}

	fmt.Println(string(kedavroJSONResult))
	// this will print: {"name": {"string": "Voldemort"}, "id": 66666, "timestamp": 1571128870000}

	// use goavro to test the generated avroJSON is valid for the schema
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		// Error parsing schema
		return err
	}

	textual, err := codec.TextualFromNative(nil, avroJSON)
	if err != nil {
		// Error avroJSON
		return err
	}

	fmt.Println(string(textual))
	// this will print: {"name": {"string": "Voldemort"}, "id": 66666, "timestamp": 1571128870000}
	return nil
}
```

### Options

`avro-kedavro` supports 4 different options for now:

* `WithStringToNumber()` will try to parse strings as numbers: `{"test": "1234.56"}` => `{"test": 1234.56}`
* `WithStringToBool()` will try to parse strings as booleans: `{"test": "False"}` => `{"test": false}`
* `WithTimestampToMillis()` will add milliseconds to timestamps, only works for `logicalType="timestamp-millis"` fields: `{"test": 1571128870}` => `{"test": time.Time(1571128870000)}`
* `WithTimestampToMicros()` will add microseconds to timestamps, only works for `logicalType="timestamp-micros"` fields: `{"test": 1571128870}` => `{"test": time.Time(1571128870000000)}`
* `WithDateTimeFormat(format string)` will try to parse a string to a timestamp using the format specified as param, only works for `logicalType="timestamp-millis"` or `logicalType="timestamp-micros"` fields: `{"test": "2019-10-14T12:45:18Z"}` => (using `time.RFC3339` as format and type `logicalType="timestamp-millis`) => `{"test": time.Time(15710571180000)}`

### Supported types

Not all the avro types are supported by `avro-kedavro` yet! The current supported types are:

| Avro      | Go                       |
| --------- | ------------------------ |
| `null`    | `nil`                    |
| `boolean` | `bool`                   |
| `bytes`   | `[]byte`                 |
| `float`   | `float32`                |
| `double`  | `float64`                |
| `long`    | `int64`                  |
| `int`     | `int32`                  |
| `string`  | `string`                 |
| `union`   | *see below*              |
| `record`  | `map[string]interface{}` |

Unsupported types:

| Avro               | 
| ------------------ |
| `enum`             |
| `fixed`            |
| `map`              |
| `array`            |

### Supported Unions

Only unions with two elements where the first one is null and the second is a supported type different than record are currently supported by `avro-kedavro`:

| First field | Second field |
| ----------- | ------------ |
| `null`      | `boolean`    |
| `null`      | `bytes`      |
| `null`      | `float`      |
| `null`      | `double`     |
| `null`      | `long`       |
| `null`      | `int`        |
| `null`      | `string`     |

### Supported Logical Types

For now only two logical types are supported:

| Avro               | Go          |
| ------------------ | ----------- |
| `timestamp-millis` | `time.Time` |
| `timestamp-micros` | `time.Time` |

#### About timestamps

For logical types of type timestamp, the schema has to be defined always as a long.

Accepted values in json for timestamps are:

* Numeric values: for example `1586502702` will be accepted as a timestamp, if a numeric value has decimals, those decimals will be ignored when parsing to `time.Time`
* Strings: only if `WithStringToNumber()` option is provided, the string will be parsed like:
  * If the string is a number without decimals: it will be treated as a timestamp (in seconds, milliseconds, or microseconds depending on the provided options to the parser)
  * If the string is a number with decimal: it will be treated as a timestamp where the decimals will be consider fractions of seconds.
    * If the selected type is `timestamp-millis` the parser will keep the first three decimals.
    * If the selected type is `timestamp-micros` the parser will keep the first six decimals.
  * If the string has non-numeric characters: the parser will try to parse the string to `time.Time` using the provided format with the option `WithDateTimeFormat(format string)`