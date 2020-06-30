package main

import (
	"encoding/json"
	"fmt"
)

func convertToJson(record map[interface{}]interface{}) ([]byte, error) {
	jsonMap := make(map[string]interface{})
	for k, v := range record {
		switch t := v.(type) {
		case []byte:
			// avoid json.Marshall encodes map's value to base64strings.
			jsonMap[k.(string)] = string(t)
		// nested json
		case map[interface{}]interface{}:
			value, err := convertToJson(t)
			if err != nil {
				return nil, err
			}
			jsonMap[k.(string)] = value
		default:
			jsonMap[k.(string)] = t
		}
	}
	b, err := json.Marshal(jsonMap)
	if err != nil {
		fmt.Errorf("flb record to json MarshallError: %s", err)
		return nil, err
	}
	return b, nil
}
