package main

import (
	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMain(m *testing.M) {
	// preprocess
	var topic *pubsub.Topic
	m.Run()
	// postprocess
}

func TestFLBPluginFlush(t *testing.T) {

}
