package main

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewOutPubSubClient(t *testing.T) {
	assert := assert.New(t)
	// case required fields missing.
	_, err := NewOutPubSubClient("", "", "")
	assert.Error(err)

	// prepare input values(FIXME: TestMain)
	projectId := os.Getenv("FLB_GPUBSUB_PROJECT_ID")
	topicId := os.Getenv("FLB_GPUBSUB_TOPIC_ID")
	keyPath := os.Getenv("FLB_GPUBSUB_KEY_PATH")

	// case wrong keyPath
	_, err = NewOutPubSubClient(projectId, topicId, "")
	assert.Error(err)

	// case correct
	_, err = NewOutPubSubClient(projectId, topicId, keyPath)
	assert.NoError(err)
}

func TestOutPubSubClient_IsTopicExists(t *testing.T) {
	// prepare input values
	projectId := os.Getenv("FLB_GPUBSUB_PROJECT_ID")
	topicId := os.Getenv("FLB_GPUBSUB_TOPIC_ID")
	keyPath := os.Getenv("FLB_GPUBSUB_KEY_PATH")

	// case topicId value is empty.
	c, _ := NewOutPubSubClient(projectId, "", keyPath)
	assert.False(t, c.IsTopicExists())

	// case correct
	c, _ = NewOutPubSubClient(projectId, topicId, keyPath)
	assert.True(t, c.IsTopicExists())

}

// TODO https://cloud.google.com/pubsub/docs/emulator?hl=ja
//func TestOutPubSubClient_Publish(t *testing.T) {
//
//}
