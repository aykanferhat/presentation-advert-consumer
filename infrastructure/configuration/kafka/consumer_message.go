package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"presentation-advert-consumer/util"
	"strconv"
)

type ConsumerMessage struct {
	*sarama.ConsumerMessage
	GroupId string
}

func getRetriedCount(message *ConsumerMessage) int {
	return getHeaderIntValue(message, RetryTopicCountKey) + 1
}

func getErrorCount(message *ConsumerMessage) int {
	return getHeaderIntValue(message, ErrorTopicCountKey)
}

func getErrorMessage(message *ConsumerMessage) string {
	return getHeaderStrValue(message, ErrorMessageKey)
}

func getTargetTopic(message *ConsumerMessage) string {
	return getHeaderStrValue(message, TargetTopicKey)
}

func getHeaderIntValue(message *ConsumerMessage, key ContextKey) int {
	value := getHeaderValue(message, key)
	if value == nil {
		return 0
	}
	count, _ := strconv.Atoi(string(value))
	return count
}

func getHeaderStrValue(message *ConsumerMessage, key ContextKey) string {
	value := getHeaderValue(message, key)
	if value == nil {
		return ""
	}
	return string(value)
}

func getHeaderValue(message *ConsumerMessage, key ContextKey) []byte {
	for _, header := range message.Headers {
		if ContextKey(header.Key) == key {
			return header.Value
		}
	}
	return nil
}

func headersForRetry(message *ConsumerMessage, errorMessage string) []sarama.RecordHeader {
	messageHeaderMap := make(map[ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[ErrorMessageKey] = util.ToByte(errorMessage)
	messageHeaderMap[RetryTopicCountKey] = util.ToByte("0")

	return mapToHeaderArray(messageHeaderMap)
}

func headersForError(message *ConsumerMessage, errorMessage string) []sarama.RecordHeader {
	messageHeaderMap := make(map[ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[ErrorMessageKey] = util.ToByte(errorMessage)
	messageHeaderMap[ErrorTopicCountKey] = util.ToByte("0")

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromRetryToRetry(message *ConsumerMessage, errorMessage string, retriedCount int) []sarama.RecordHeader {
	messageHeaderMap := make(map[ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[ContextKey(header.Key)] = header.Value
	}
	messageHeaderMap[ErrorMessageKey] = util.ToByte(errorMessage)
	messageHeaderMap[RetryTopicCountKey] = util.ToByte(fmt.Sprint(retriedCount))

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromRetryToError(message *ConsumerMessage, errorMessage string) []sarama.RecordHeader {
	messageHeaderMap := make(map[ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[ContextKey(header.Key)] = header.Value
	}
	errorCount := getErrorCount(message)
	messageHeaderMap[ErrorTopicCountKey] = util.ToByte(fmt.Sprint(errorCount + 1))
	messageHeaderMap[ErrorMessageKey] = util.ToByte(errorMessage)
	messageHeaderMap[TargetTopicKey] = util.ToByte(message.Topic)
	delete(messageHeaderMap, RetryTopicCountKey)

	return mapToHeaderArray(messageHeaderMap)
}

func headersFromErrorToRetry(message *ConsumerMessage) []sarama.RecordHeader {
	messageHeaderMap := make(map[ContextKey][]byte)
	for _, header := range message.Headers {
		messageHeaderMap[ContextKey(header.Key)] = header.Value
	}
	delete(messageHeaderMap, RetryTopicCountKey)

	return mapToHeaderArray(messageHeaderMap)
}

func mapToHeaderArray(messageHeaderMap map[ContextKey][]byte) []sarama.RecordHeader {
	headers := make([]sarama.RecordHeader, 0)
	for key, bytes := range messageHeaderMap {
		headers = append(headers, sarama.RecordHeader{
			Key:   util.ToByte(key.String()),
			Value: bytes,
		})
	}
	return headers
}

func isMainTopic(message *ConsumerMessage, consumerTopicConfig *ConsumerGroupConfig) bool {
	return message.Topic == consumerTopicConfig.Name
}

func isRetryTopic(message *ConsumerMessage, consumerTopicConfig *ConsumerGroupConfig) bool {
	return message.Topic == consumerTopicConfig.Retry
}
