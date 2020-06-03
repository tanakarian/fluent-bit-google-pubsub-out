package main

import (
	"encoding/json"
	"fmt"
)

func ConvertToJson(record map[interface{}]interface{}) []byte {
	jsonMap := make(map[string]interface{})
	for k, v := range record {
		switch t := v.(type) {
		case []byte:
			// []byteだとjson.Marshall時にbase64encodeされるため
			jsonMap[k.(string)] = string(t)
		// nested json
		case map[interface{}]interface{}:
			jsonMap[k.(string)] = ConvertToJson(t)
		default:
			jsonMap[k.(string)] = t
		}
	}
	b, err := json.Marshal(jsonMap)
	if err != nil {
		fmt.Println("flb record to json MarshallError: ", err)
	}
	return b
}
