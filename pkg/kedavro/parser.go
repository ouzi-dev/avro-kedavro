package kedavro

import (
	"encoding/json"
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
)

type parser struct {
	schema *schema.Field
}

type Parser interface {
	Parse(record []byte) (interface{}, error)
}

func NewParser(schemaString string) (Parser, error) {
	s := map[string]interface{}{}

	if err := json.Unmarshal([]byte(schemaString), &s); err != nil {
		return nil, fmt.Errorf("unmarshall schema failed: %v", err)
	}

	rootField, err := schema.ParseSchemaField(s)
	if err != nil {
		return nil, err
	}

	if rootField.Type != schema.Primitive || rootField.TypeValue.(string) != "record" {
		return nil, fmt.Errorf("schema root field must be of type record")
	}

	return &parser{
		schema: rootField,
	}, nil
}

func (p *parser) Parse(record []byte) (interface{}, error) {
	jsonRecord := map[string]interface{}{}
	if err := json.Unmarshal(record, &jsonRecord); err != nil {
		return nil, fmt.Errorf("unmarshall record failed: %v", err)
	}

	return parseRecord(p.schema, jsonRecord)
}
