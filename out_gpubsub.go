package main

import (
	"C"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"unsafe"
)
import "github.com/fluent/fluent-bit-go/output"
import "cloud.google.com/go/pubsub"

var topic *pubsub.Topic

//export FLBPluginRegister
func FLBPluginRegister(flbCtx unsafe.Pointer) int {
	return output.FLBPluginRegister(flbCtx, "gcloud_pubsub", "Output to Google Cloud PubSub")
}

//export FLBPluginInit
func FLBPluginInit(flbCtx unsafe.Pointer) int {
	// retrieve config values from conf file
	projectId := output.FLBPluginConfigKey(flbCtx, "Project")
	topicId := output.FLBPluginConfigKey(flbCtx, "Topic")
	keyPath := output.FLBPluginConfigKey(flbCtx, "Key")

	// load service account's credential file
	opt := option.WithCredentialsFile(keyPath)
	ctx := context.Background()
	pubsubClient, err := pubsub.NewClient(ctx, projectId, opt)
	if err != nil {
		fmt.Printf("[flb-go: gcloud_pubsub] initialize client err: %s\n", err)
		return output.FLB_ERROR
	}

	// type Topic is embedding pubsub.Client.
	topic = pubsubClient.Topic(topicId)
	topicExistCtx := context.Background()
	exist, existErr := topic.Exists(topicExistCtx)
	if !exist {
		fmt.Printf("[flb-go: gcloud_pubsub] topic is not found: %s\nYou must set an existing topic name\n", existErr)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	decoder := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(decoder)
		if ret != 0 {
			break
		}

		jsonBytes := ConvertToJson(record)
		// TODO: initialize with required fields
		msg := &pubsub.Message{
			Data: jsonBytes,
		}

		ctx := context.Background()
		result := topic.Publish(ctx, msg)
		if _, err := result.Get(ctx); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				fmt.Printf("[flb-go: gcloud_pubsub] flb engine will retry. msg: %v", msg)
				// flb engine controls retry wait time for exponential backoff.
				return output.FLB_RETRY
			}
			// By default, return error
			fmt.Printf("[flb-go: gcloud_pubsub] publish err: %s", err)
			return output.FLB_ERROR
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	// When Stop() is called, then flb engine flushes bundles(handle remaining batch of records) and stops a scheduler.
	topic.Stop()
	return output.FLB_OK
}

func main() {
}
