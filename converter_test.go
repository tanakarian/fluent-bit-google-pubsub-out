package main

import (
    "github.com/stretchr/testify/assert"
    "testing"
)

func TestConvertToJson(t *testing.T) {
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
                "key3": []byte("value3"),
            },
        },
        {
            // map[interface{}]interface{}
            inputType: "map[interface{}]interface{}",
            m: map[interface{}]interface{}{
                "key1": map[interface{}]interface{}{},
                "key2": map[interface{}]interface{}{},
                "key3": map[interface{}]interface{}{},
            },
        },
        {
            // default
            // int, uint, int32, int64, uint32, uint64, string, etc...
            inputType: "[]byte",
            m: map[interface{}]interface{}{
                "key1": "value1",
                "key2": []byte("value2"),
                "key3": []byte("value3"),
            },
        },
    }

    for _, item := range table {
        t.Run(item.inputType, func(t *testing.T) {
            _, err := convertToJson(item.m)
            assert.NoError(err)
        })
    }
}
