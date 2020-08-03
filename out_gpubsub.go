package main

import (
	"C"
	"context"
	"strconv"
	"time"
	"unsafe"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"

	"cloud.google.com/go/pubsub"
)

var outpubsubclient *OutPubSubClient
var logger *logrus.Logger

func init() {
	logger = logrus.New()
	// TODO enable users of plugin to set logLevel via config file.
	level, _ := logrus.ParseLevel("info")
	logger.SetLevel(level)
	formatter := &flbFormat{TimestampFormat: "2006/01/02 15:04:05"}
	logger.SetFormatter(formatter)
}

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
		logger.Error("[flb-go::gcloud_pubsub] projectId, topicId, keyPath are required fields")
		return output.FLB_ERROR
	}

	c, err := NewOutPubSubClient(projectID, topicID, keyPath)
	if err != nil {
		logger.Errorf("%+v", err)
		return output.FLB_ERROR
	}

	outpubsubclient = c
	if ok, existErr := outpubsubclient.IsTopicExists(); !ok {
		logger.Errorf("%+v", existErr)
		return output.FLB_ERROR
	}

	// configure publishSettings
	if delayThreshold != "" {
		v, err := strconv.Atoi(delayThreshold)
		if err != nil {
			logger.Errorf("param \"delayThreshold\" is not valid. %+v", err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicDelayThreshold(time.Duration(v) * time.Millisecond)
	}

	if countThreshold != "" {
		v, err := strconv.Atoi(countThreshold)
		if err != nil {
			logger.Errorf("param \"countThreshold\" is not valid. %+v", err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicCountThreshold(v)
	}
	if byteThreshold != "" {
		v, err := strconv.Atoi(byteThreshold)
		if err != nil {
			logger.Errorf("param \"byteThreshold\" is not valid. %+v", err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicByteThreshold(v)
	}

	if numGoroutines != "" {
		v, err := strconv.Atoi(numGoroutines)
		if err != nil {
			logger.Errorf("param \"numGoroutines\" is not valid. %+v", err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicNumGoroutines(v)
	}

	if timeout != "" {
		v, err := strconv.Atoi(timeout)
		if err != nil {
			logger.Errorf("param \"timeout\" is not valid. %+v", err)
			return output.FLB_ERROR
		}
		outpubsubclient.SetTopicTimeout(time.Duration(v) * time.Second)
	}

	if bufferedByteLimit != "" {
		v, err := strconv.Atoi(bufferedByteLimit)
		if err != nil {
			logger.Errorf("param \"bufferedByteLimit\" is not valid. %+v", err)
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

		jsonBytes, err := convertToJSON(record)
		if err != nil {
			logger.Errorf("%+v", err)
			return output.FLB_ERROR
		}

		msg := &pubsub.Message{
			Data: jsonBytes,
		}

		publishResults = append(publishResults, outpubsubclient.Publish(ctx, msg))
	}

	for _, result := range publishResults {
		if _, err := result.Get(ctx); err != nil {
			if err == context.DeadlineExceeded {
				logger.Warn("[flb-go::gcloud_pubsub] Deadline Exceeded. will retry...")
				// flb scheduler retries failed task by using backoff_full_jitter algorithm.
				return output.FLB_RETRY
			}

			if stts, ok := status.FromError(err); !ok {
				logger.Errorf("[flb-go::gcloud_pubsub] Could not parse error to grpc.status. err: %+v", err)
				return output.FLB_ERROR
			} else {
				switch stts.Code() {
				// See here: https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto
				case codes.DeadlineExceeded, codes.Internal, codes.Unavailable:
					logger.Warnf("[flb-go::gcloud_pubsub] Retryable error: %s. will retry...", stts.Err())
					return output.FLB_RETRY
				default:
					logger.Errorf("[flb-go::gcloud_pubsub] Publish Error: %s. will retry... %+v", stts.Err())
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
