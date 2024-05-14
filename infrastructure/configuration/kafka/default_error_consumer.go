package kafka

import (
	"context"
)

type defaultErrorConsumer struct {
	producer SyncProducer
}

func NewDefaultErrorConsumer(
	producer SyncProducer,
) Consumer {
	return &defaultErrorConsumer{
		producer: producer,
	}
}

func (consumer *defaultErrorConsumer) Consume(_ context.Context, message *ConsumerMessage) error {
	targetTopic := getTargetTopic(message)
	if targetTopic == "" {
		return nil
	}
	return sendMessageToTopic(consumer.producer, message, targetTopic, headersFromErrorToRetry(message))
}
