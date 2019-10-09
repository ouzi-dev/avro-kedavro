package parser

import "fmt"

func parseStringField(name string, value interface{}) (string, error) {
	v, ok := value.(string)

	if !ok {
		return "", fmt.Errorf("value \"%v\" in field \"%s\" in not of type \"string\"", value, name)
	}

	return v, nil
}
