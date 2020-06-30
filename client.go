package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"google.golang.org/api/option"
)

type OutPubSubClient struct {
	pubsubclient *pubsub.Client
	topic        *pubsub.Topic // type Topic is embedding pubsub.Client.
}

func NewOutPubSubClient(projectId string, topicId string, keyPath string) (*OutPubSubClient, error) {
	if projectId == "" || topicId == "" || keyPath == "" {
		return nil, fmt.Errorf("[flb-go::gcloud_pubsub] projectId, topicId, keyPath are required fields")
	}

	opt := option.WithCredentialsFile(keyPath)
	ctx := context.Background()
	pubsubc, err := pubsub.NewClient(ctx, projectId, opt)
	if err != nil {
		return nil, fmt.Errorf("[flb-go::gcloud_pubsub] initialize pubsub client err: %s\n", err)
	}

	topic := pubsubc.Topic(topicId)
	client := &OutPubSubClient{
		pubsubclient: pubsubc,
		topic:        topic,
	}

	return client, nil
}

func (c *OutPubSubClient) IsTopicExists() bool {
	ctx := context.Background()
	exist, err := c.topic.Exists(ctx)
	if !exist {
		fmt.Printf("[flb-go::gcloud_pubsub] topic is not found: %s. You must set an existing topic name\n", err)
		return false
	}

	return true
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