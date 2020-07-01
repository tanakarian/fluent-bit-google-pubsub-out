package main

import (
	"C"
	"context"
	"fmt"
	"unsafe"
)
import "github.com/fluent/fluent-bit-go/output"
import "cloud.google.com/go/pubsub"

var outpubsubclient *OutPubSubClient

//export FLBPluginRegister
func FLBPluginRegister(flbCtx unsafe.Pointer) int {
	return output.FLBPluginRegister(flbCtx, "gcloud_pubsub", "Output to Google Cloud PubSub")
}

//export FLBPluginInit
func FLBPluginInit(flbCtx unsafe.Pointer) int {
	// required
	projectId := output.FLBPluginConfigKey(flbCtx, "Project")
	topicId := output.FLBPluginConfigKey(flbCtx, "Topic")
	keyPath := output.FLBPluginConfigKey(flbCtx, "Key")

	c, err := NewOutPubSubClient(projectId, topicId, keyPath)
	if err != nil {
		return output.FLB_ERROR
	}
	outpubsubclient = c

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

		jsonBytes, err := convertToJson(record)
		if err != nil {
			fmt.Printf("[flb-go::gcloud_pubsub] parse error: %s", err)
			return output.FLB_ERROR
		}

		msg := &pubsub.Message{
			Data: jsonBytes,
		}

		ctx := context.Background()
		result := outpubsubclient.Publish(ctx, msg)
		if _, err = result.Get(ctx); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				fmt.Printf("[flb-go::gcloud_pubsub] flb engine will retry. msg: %v", msg)
				return output.FLB_RETRY // flb scheduler retries failed task by using backoff_full_jitter algorithm.
			}

			fmt.Printf("[flb-go::gcloud_pubsub] publish err: %s", err)
			return output.FLB_ERROR
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	outpubsubclient.Stop()
	outpubsubclient.Close()
	return output.FLB_OK
}

func main() {
}
