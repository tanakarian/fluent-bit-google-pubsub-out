package main

import (
	"encoding/json"
)

func convertToJSON(record map[interface{}]interface{}) ([]byte, error) {
	jsonMap := makeJSONMap(record)

	b, err := json.Marshal(jsonMap)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func makeJSONMap(record map[interface{}]interface{}) map[string]interface{} {
	jsonMap := make(map[string]interface{})
	for k, v := range record {
		switch t := v.(type) {
		case []byte:
			// avoid json.Marshall encodes map's value to base64strings.
			jsonMap[k.(string)] = string(t)
		// nested json
		case map[interface{}]interface{}:
			value := makeJSONMap(t)
			jsonMap[k.(string)] = value
		default:
			jsonMap[k.(string)] = t
		}
	}
	return jsonMap
}
