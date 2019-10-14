package kedavro

import (
	"fmt"

	"github.com/linkedin/goavro"
	"github.com/ouzi-dev/avro-kedavro/pkg/schema"
	"github.com/ouzi-dev/avro-kedavro/pkg/types"
)

func parseUnionField(field *schema.Field, record map[string]interface{}) (interface{}, error) {
	/*
	 * How to deal with unions the easy way:
	 * for now we have only unions ["null", "supported_type"] where the default can only be null
	 * so, if we don't have value or it's nil we just return nil
	 * though we could treat it as a nil field with default value if the union has a default value
	 * And if it's a different type, it's a field of that type without default
	 * This should work to support unions with multiple types just checking the type of the current
	 * value to match the first type possible in the types array
	 */

	value, ok := record[field.Name]
	if !ok {
		if !field.HasDefault {
			return nil, fmt.Errorf("value for field \"%s\" not found", field.Name)
		}
		// we already validate this... so defaultValue is null here
		return nil, nil
	}

	// so we have something, for now only two options, so let's check null first
	if value == nil {
		return nil, nil
	}

	// now, it's not null... so it has to be of type field.TypeValue[1]!
	// so let's create a new field!
	// we can do this safely cause we already validated this on the package schema
	typeArray := field.TypeValue.([]interface{})
	searchedType := typeArray[1].(string)
	unionField := &schema.Field{
		Name:      field.Name,
		Type:      types.Primitive,
		TypeValue: searchedType,
		// TODO: support logicaltypes in unions, for now leave it
		// this is wrong we should here searchedType["logicaltype"]
		LogicalType: field.LogicalType,
		// TODO: support record type in unions
		// same as before, I think this should be something like searchedType["fields"]
		Fields: []interface{}{},
		// only the first item of the union can have default
		HasDefault: false,
	}

	parsedValue, err := parsePrimitiveField(unionField, record)

	if err != nil {
		return nil, err
	}

	return goavro.Union(searchedType, parsedValue), nil
}
