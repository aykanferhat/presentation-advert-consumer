package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"time"
)

type consumerGroupHandler interface {
	Setup(sarama.ConsumerGroupSession) error
	Cleanup(sarama.ConsumerGroupSession) error
	ConsumeClaim(sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) error
	GetState() *ConsumerGroupHandlerState
}

type consumerGroupHandlerImpl struct {
	consumerTopicConfig *ConsumerGroupConfig
	consumers           *ConsumerGroupConsumers
	producer            SyncProducer

	state *ConsumerGroupHandlerState
}

func newConsumerGroupHandlerImpl(
	consumerTopicConfig *ConsumerGroupConfig,
	consumers *ConsumerGroupConsumers,
	producer SyncProducer,
) consumerGroupHandler {
	return &consumerGroupHandlerImpl{
		consumerTopicConfig: consumerTopicConfig,
		consumers:           consumers,
		producer:            producer,
		state: &ConsumerGroupHandlerState{
			GroupId:             consumerTopicConfig.GroupId,
			Status:              ConsumerGroupHandlerCreated,
			ConsumerTopicStates: make(map[string]*ConsumerGroupHandlerTopicState),
		},
	}
}

func (handler *consumerGroupHandlerImpl) Setup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			topicPartitionKey := getTopicPartitionKey(topic, partition)
			handler.state.ConsumerTopicStates[topicPartitionKey] = &ConsumerGroupHandlerTopicState{
				Topic:     topic,
				Partition: partition,
				Status:    ConsumerGroupHandlerTopicCreated,
			}
		}
	}
	handler.state.Status = ConsumerGroupHandlerStarted
	return nil
}

func (handler *consumerGroupHandlerImpl) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic := claim.Topic()
	partition := claim.Partition()
	key := getTopicPartitionKey(topic, partition)
	state := handler.state.ConsumerTopicStates[key]
	state.Status = ConsumerGroupHandlerTopicListening
	state.ListeningDate = time.Now()
	consumer := handler.consumers.Consumer
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				continue
			}
			state.Status = ConsumerGroupHandlerTopicStarted
			state.LatestConsumedOffset = message.Offset
			state.LatestConsumedDate = time.Now()

			consumerMessage := &ConsumerMessage{ConsumerMessage: message, GroupId: handler.consumerTopicConfig.GroupId}
			if err := processMessage(context.Background(), consumer, consumerMessage, handler.consumerTopicConfig.MaxProcessingTime); err != nil {
				processConsumedMessageError(context.Background(), consumerMessage, err, handler.producer, handler.consumerTopicConfig)
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			state.Status = ConsumerGroupHandlerTopicClosed
			return nil
		}
	}
}

func (handler *consumerGroupHandlerImpl) Cleanup(sarama.ConsumerGroupSession) error {
	handler.state.Status = ConsumerGroupHandlerClosed
	for _, state := range handler.state.ConsumerTopicStates {
		state.Status = ConsumerGroupHandlerTopicClosed
		state.ClosedDate = time.Now()
	}
	return nil
}

func (handler *consumerGroupHandlerImpl) GetState() *ConsumerGroupHandlerState {
	return handler.state
}
