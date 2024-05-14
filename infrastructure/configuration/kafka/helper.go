package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"strings"
	"time"
)

const subscribeErr = "Error from consumerGroup group: %s, err: %s"

func subscribe(saramaConfig *sarama.Config, clusterConfig *ClusterConfig, consumerGroupHandler sarama.ConsumerGroupHandler, consumerGroupId string, topics map[string]struct{}, isErrorConsumer bool) (sarama.Client, sarama.ConsumerGroup, error) {
	saramaClient, err := NewClient(saramaConfig, clusterConfig)
	if err != nil {
		return nil, nil, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroupId, saramaClient)
	if err != nil {
		return nil, nil, err
	}
	topicsArray := make([]string, 0, len(topics))
	for topic := range topics {
		topicsArray = append(topicsArray, topic)
	}
	ctx := context.Background()
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, topicsArray, consumerGroupHandler); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					break
				}
				log.Errorf(subscribeErr, consumerGroupId, err.Error())
				continue
			}
			if ctx.Err() != nil {
				log.Errorf(subscribeErr, consumerGroupId, ctx.Err().Error())
				continue
			}
			if isErrorConsumer { // we don't want to start again when rebalance or else.
				break
			}
		}
	}()
	go func() {
		for err := range consumerGroup.Errors() {
			log.Errorf(subscribeErr, consumerGroupId, err.Error())
		}
	}()
	return saramaClient, consumerGroup, nil
}

var clientClosedErr = "kafka: tried to use a client that was closed"

func unsubscribe(client sarama.Client, consumerGroup sarama.ConsumerGroup) error {
	if client != nil {
		if err := client.Close(); err != nil {
			if !strings.EqualFold(err.Error(), clientClosedErr) {
				return err
			}
		}
	}
	if consumerGroup != nil {
		if err := consumerGroup.Close(); err != nil {
			if !strings.EqualFold(err.Error(), clientClosedErr) {
				return err
			}
		}
	}
	return nil
}

func processMessage(ctx context.Context, consumer Consumer, message *ConsumerMessage, maxProcessingTime time.Duration) error {
	contextWithTimeout, cancel := context.WithTimeout(ctx, maxProcessingTime)
	defer cancel()
	resultChan := make(chan error, 1)
	go func(r chan<- error) {
		r <- consumer.Consume(contextWithTimeout, message)
	}(resultChan)
	select {
	case or := <-resultChan:
		return or
	case <-contextWithTimeout.Done():
		return contextWithTimeout.Err()
	}
}

func processConsumedMessageError(ctx context.Context, message *ConsumerMessage, err error, producer SyncProducer, consumerTopicConfig *ConsumerGroupConfig) {
	if isMainTopic(message, consumerTopicConfig) {
		processConsumedMainTopicMessageError(ctx, message, err, producer, consumerTopicConfig)
		return
	}

	if isRetryTopic(message, consumerTopicConfig) {
		processConsumedRetryTopicMessageError(ctx, message, err, producer, consumerTopicConfig)
		return
	}
}

func processConsumedMainTopicMessageError(ctx context.Context, message *ConsumerMessage, err error, producer SyncProducer, consumerTopicConfig *ConsumerGroupConfig) {
	if consumerTopicConfig.IsNotDefinedRetryAndErrorTopic() {
		return
	}
	if consumerTopicConfig.IsNotDefinedRetryTopic() && consumerTopicConfig.IsDefinedErrorTopic() {
		messageSendError := sendMessageToTopic(producer, message, consumerTopicConfig.Error, headersForError(message, err.Error()))
		if messageSendError != nil {
			log.Errorf("An error occurred when sent error message to error topic: %s, err: %s", consumerTopicConfig.Error, messageSendError.Error())
		}
		return
	}
	messageSendError := sendMessageToTopic(producer, message, consumerTopicConfig.Retry, headersForRetry(message, err.Error()))
	if messageSendError != nil {
		joinedErr := errors.Join(err, messageSendError)
		log.Errorf("An error occurred when sent error message to error topic: %s, err: %s", consumerTopicConfig.Error, joinedErr)
	}
}

func processConsumedRetryTopicMessageError(ctx context.Context, message *ConsumerMessage, err error, producer SyncProducer, consumerTopicConfig *ConsumerGroupConfig) {
	retriedCount := getRetriedCount(message)
	if retriedCount >= consumerTopicConfig.RetryCount && consumerTopicConfig.IsNotDefinedErrorTopic() {
		return
	}
	if retriedCount >= consumerTopicConfig.RetryCount && consumerTopicConfig.IsDefinedErrorTopic() {
		reachedMaxRetryCountErr := fmt.Errorf("reached max rety count, retriedCount: %d", retriedCount)
		joinedErr := errors.Join(err, reachedMaxRetryCountErr)
		messageSendError := sendMessageToTopic(producer, message, consumerTopicConfig.Error, headersFromRetryToError(message, joinedErr.Error()))
		if messageSendError != nil {
			joinedErr = errors.Join(joinedErr, messageSendError)
			log.Errorf("An error occurred when sent error message to error topic: %s, err: %s", consumerTopicConfig.Error, joinedErr)
		}
		return
	}
	messageSendError := sendMessageToTopic(producer, message, consumerTopicConfig.Retry, headersFromRetryToRetry(message, err.Error(), retriedCount))
	if messageSendError != nil {
		joinedErr := errors.Join(err, messageSendError)
		log.Errorf("An error occurred when sent error message to error topic: %s, err: %s", consumerTopicConfig.Error, joinedErr)
	}
}

func sendMessageToTopic(producer SyncProducer, message *ConsumerMessage, topic string, headers []sarama.RecordHeader) error {
	_, _, err := producer.SendMessage(&ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(message.Key),
		Value:   sarama.StringEncoder(message.Value),
		Headers: headers,
	})
	return err
}

func getTopicPartitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s_%d", topic, partition)
}
