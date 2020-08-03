package main

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/pkg/errors"
)

type OutPubSubClient struct {
	pubsubclient *pubsub.Client
	topic        *pubsub.Topic // type Topic is embedding pubsub.Client.
}

func NewOutPubSubClient(projectID string, topicID string, keyPath string) (*OutPubSubClient, error) {
	opt := option.WithCredentialsFile(keyPath)
	ctx := context.Background()
	pubsubc, err := pubsub.NewClient(ctx, projectID, opt)
	if err != nil {
		return nil, errors.Wrap(err, "[flb-go::gcloud_pubsub] Initialize pubsub client err")
	}

	topic := pubsubc.Topic(topicID)

	client := &OutPubSubClient{
		pubsubclient: pubsubc,
		topic:        topic,
	}

	return client, nil
}

func (c *OutPubSubClient) IsTopicExists() (bool, error) {
	ctx := context.Background()
	exist, err := c.topic.Exists(ctx)
	if !exist {
		return false, errors.Wrap(err, "[flb-go::gcloud_pubsub] Topic does not exist")
	}

	return true, nil
}

func (c *OutPubSubClient) SetTopicDelayThreshold(v time.Duration) {
	c.topic.PublishSettings.DelayThreshold = v
}

func (c *OutPubSubClient) SetTopicCountThreshold(v int) {
	c.topic.PublishSettings.CountThreshold = v
}

func (c *OutPubSubClient) SetTopicByteThreshold(v int) {
	c.topic.PublishSettings.ByteThreshold = v
}

func (c *OutPubSubClient) SetTopicNumGoroutines(v int) {
	c.topic.PublishSettings.NumGoroutines = v
}

func (c *OutPubSubClient) SetTopicTimeout(v time.Duration) {
	c.topic.PublishSettings.Timeout = v
}

func (c *OutPubSubClient) SetTopicBufferedByteLimit(v int) {
	c.topic.PublishSettings.BufferedByteLimit = v
}

func (c *OutPubSubClient) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	return c.topic.Publish(ctx, msg)
}

func (c *OutPubSubClient) Stop() {
	// When Stop() is called, then flush bundles(handle remaining batch of records) and stop a scheduler.
	c.topic.Stop()
}

func (c *OutPubSubClient) Close() error {
	return c.pubsubclient.Close()
}
