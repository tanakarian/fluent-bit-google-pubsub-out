package main

import (
	"C"
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"unsafe"

	"strconv"

	"github.com/fluent/fluent-bit-go/output"

	"cloud.google.com/go/pubsub"
)
import "time"

var outpubsubclient *OutPubSubClient

//export FLBPluginRegister
func FLBPluginRegister(flbCtx unsafe.Pointer) int {
	return output.FLBPluginRegister(flbCtx, "gcloud_pubsub", "Output to Google Cloud PubSub")
}

//export FLBPluginInit
func FLBPluginInit(flbCtx unsafe.Pointer) int {
	// required
	projectID := output.FLBPluginConfigKey(flbCtx, "Project")
	topicID := output.FLBPluginConfigKey(flbCtx, "Topic")
	keyPath := output.FLBPluginConfigKey(flbCtx, "Key")
	// optional
	delayThreshold := output.FLBPluginConfigKey(flbCtx, "DelayThreshold")
	countThreshold := output.FLBPluginConfigKey(flbCtx, "CountThreshold")
	byteThreshold := output.FLBPluginConfigKey(flbCtx, "ByteThreshold")
	numGoroutines := output.FLBPluginConfigKey(flbCtx, "NumGoroutines")
	timeout := output.FLBPluginConfigKey(flbCtx, "Timeout")
	bufferedByteLimit := output.FLBPluginConfigKey(flbCtx, "BufferedByteLimit")

	if projectID == "" || topicID == "" || keyPath == "" {
		fmt.Println(fmt.Errorf("[flb-go::gcloud_pubsub] projectId, topicId, keyPath are required fields"))
		return output.FLB_ERROR
	}

	c, err := NewOutPubSubClient(projectID, topicID, keyPath)
	if err != nil {
		fmt.Println(err)
		return output.FLB_ERROR
	}

	outpubsubclient = c
	if ok, existErr := outpubsubclient.IsTopicExists(); !ok {
		fmt.Printf("[flb-go::gcloud_pubsub] topic is not found: %s. You must set an existing topic name\n", existErr)
		return output.FLB_ERROR
	}

	// configure publishSettings
	if delayThreshold != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicDelayThreshold(time.Duration(v) * time.Millisecond)
	}

	if countThreshold != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicCountThreshold(v)
	}
	if byteThreshold != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicByteThreshold(v)
	}

	if numGoroutines != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicNumGoroutines(v)
	}

	if timeout != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicTimeout(time.Duration(v) * time.Millisecond)
	}

	if bufferedByteLimit != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			fmt.Println(err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicBufferedByteLimit(v)
	}

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	ctx := context.Background()
	var publishResults []*pubsub.PublishResult
	decoder := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(decoder)
		if ret != 0 {
			break
		}

		// for debug
		fmt.Println("[flb-go::gcloud_pubsub] run flush. next convertToJSON")
		jsonBytes, err := convertToJSON(record)
		if err != nil {
			fmt.Printf("[flb-go::gcloud_pubsub] parse error: %s", err)
			return output.FLB_ERROR
		}

		msg := &pubsub.Message{
			Data: jsonBytes,
		}

		publishResults = append(publishResults, outpubsubclient.Publish(ctx, msg))
	}

	for _, result := range publishResults {
		if _, err := result.Get(ctx); err != nil {
			if stts, ok := status.FromError(err); !ok {
				fmt.Printf("[flb-go::gcloud_pubsub] unexpected error: %s", err)
			} else {
				switch stts.Code() {
				case codes.DeadlineExceeded, codes.Internal, codes.Unavailable:
					fmt.Printf("[flb-go::gcloud_pubsub] flb engine will retry. err: %s", stts.Err())
					return output.FLB_RETRY // flb scheduler retries failed task by using backoff_full_jitter algorithm.
				default:
					fmt.Printf("[flb-go::gcloud_pubsub] publish err: %s", stts.Err())
					return output.FLB_ERROR
				}
			}
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
