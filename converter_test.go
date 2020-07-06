package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertToJSON(t *testing.T) {
	assert := assert.New(t)
	// table driven tests
	table := []struct {
		inputType string
		m         map[interface{}]interface{} // flb data map's key is assumed to be type:string.
	}{
		{
			// []byte
			inputType: "[]byte",
			m: map[interface{}]interface{}{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
			},
		},
		{
			// map[interface{}]interface{}
			inputType: "map[interface{}]interface{}",
			m: map[interface{}]interface{}{
				"key1": map[interface{}]interface{}{"key1": "value"},
				"key2": map[interface{}]interface{}{"key2": map[interface{}]interface{}{"nest1": 100}},
				"key3": map[interface{}]interface{}{"key3": map[interface{}]interface{}{"nested_key": map[interface{}]interface{}{"nest2": -100}}},
			},
		},
		{
			// default
			// int, uint, int32, int64, uint32, uint64, string, etc...
			inputType: "default",
			m: map[interface{}]interface{}{
				"key1": "value1",
				"key2": 36,
				"key3": -100000,
				"key4": 9223372036854775807,
			},
		},
	}

	for _, item := range table {
		t.Run(item.inputType, func(t *testing.T) {
			_, err := convertToJSON(item.m)
			if ok := assert.NoError(err); !ok {
				t.FailNow()
			}
		})
	}
}
