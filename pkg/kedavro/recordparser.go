package kedavro

import (
	"fmt"

	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
)

func parseRecordField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	// record is a bit different, first it doesn't use default, and second we just
	// want to check if the object exists to start again processing a new record
	value, ok := record[field.Name]
	if !ok {
		return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
	}

	valueAsMap, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"record\"", value, field.Name)
	}
	return parseRecord(field, valueAsMap)
}
