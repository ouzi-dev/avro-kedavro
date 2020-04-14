package kedavro

import (
	"encoding/json"
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/types"
)

type parser struct {
	schema *Field
}

type Parser interface {
	Parse(record []byte) (interface{}, error)
}

// ParserOption reconfigure the parser creation.
type ParserOption func(*types.Options)

func WithStringToNumber() ParserOption {
	return func(o *types.Options) { o.IsStringToNumber = true }
}

func WithStringToBool() ParserOption {
	return func(o *types.Options) { o.IsStringToBool = true }
}

func WithTimestampToMillis() ParserOption {
	return func(o *types.Options) { o.IsTimestampToMillis = true }
}

func WithTimestampToMicros() ParserOption {
	return func(o *types.Options) { o.IsTimestampToMicros = true }
}

func WithDateTimeFormat(format string) ParserOption {
	return func(o *types.Options) {
		o.IsFormatDateTime = true
		o.DateTimeFormat = format
	}
}

func WithNowForNullTimestamp() ParserOption {
	return func(o *types.Options) {
		o.IsSetNowForNilTimestamp = true
	}
}

func NewParser(schemaString string, opts ...ParserOption) (Parser, error) {
	s := map[string]interface{}{}

	if err := json.Unmarshal([]byte(schemaString), &s); err != nil {
		return nil, fmt.Errorf("unmarshall schema failed: %v", err)
	}

	options := types.Options{}

	for _, opt := range opts {
		opt(&options)
	}

	rootField, err := ParseSchemaField(s, options)
	if err != nil {
		return nil, err
	}

	if rootField.Type != types.Primitive || rootField.TypeValue.(string) != "record" {
		return nil, fmt.Errorf("schema root field must be of type record")
	}

	parser := &parser{
		schema: rootField,
	}

	return parser, nil
}

func (p *parser) Parse(record []byte) (interface{}, error) {
	jsonRecord := map[string]interface{}{}
	if err := json.Unmarshal(record, &jsonRecord); err != nil {
		return nil, fmt.Errorf("unmarshall record failed: %v", err)
	}

	return parseRecord(p.schema, jsonRecord)
}
