package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"time"
)

type errorConsumerGroupHandler struct {
	consumerGroupErrorConfig *ConsumerGroupErrorConfig
	errorTopicConsumerMap    map[string]Consumer

	//  create in newErrorConsumerGroupHandler
	state *ConsumerGroupHandlerState
}

func (handler *errorConsumerGroupHandler) GetState() *ConsumerGroupHandlerState {
	return handler.state
}

func newErrorConsumerGroupHandler(
	consumerGroupErrorConfig *ConsumerGroupErrorConfig,
	errorTopicConsumerMap map[string]Consumer,
) consumerGroupHandler {
	return &errorConsumerGroupHandler{
		consumerGroupErrorConfig: consumerGroupErrorConfig,
		errorTopicConsumerMap:    errorTopicConsumerMap,
		state: &ConsumerGroupHandlerState{
			GroupId:             consumerGroupErrorConfig.GroupId,
			Status:              ConsumerGroupHandlerCreated,
			CreatedDate:         time.Now(),
			ConsumerTopicStates: make(map[string]*ConsumerGroupHandlerTopicState),
		},
	}
}

func (handler *errorConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	for topic, partitions := range session.Claims() {
		for _, partition := range partitions {
			key := getTopicPartitionKey(topic, partition)
			handler.state.ConsumerTopicStates[key] = &ConsumerGroupHandlerTopicState{
				Topic:       topic,
				Partition:   partition,
				Status:      ConsumerGroupHandlerTopicCreated,
				CreatedDate: time.Now(),
			}
		}
	}
	handler.state.Status = ConsumerGroupHandlerStarted
	return nil
}

func (handler *errorConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic := claim.Topic()
	partition := claim.Partition()
	key := getTopicPartitionKey(topic, partition)
	state := handler.state.ConsumerTopicStates[key]
	state.Status = ConsumerGroupHandlerTopicListening
	state.ListeningDate = time.Now()
	consumer := handler.errorTopicConsumerMap[topic]
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				continue
			}
			state.LatestConsumedOffset = message.Offset
			state.LatestConsumedDate = time.Now()

			if time.Since(message.Timestamp).Nanoseconds() < handler.consumerGroupErrorConfig.CloseConsumerWhenMessageIsNew.Nanoseconds() {
				state.Status = ConsumerGroupHandlerTopicNewMessage
				continue
			}
			state.Status = ConsumerGroupHandlerTopicStarted
			consumerMessage := &ConsumerMessage{ConsumerMessage: message, GroupId: handler.consumerGroupErrorConfig.GroupId}

			ctx := context.Background()
			errorCount := getErrorCount(consumerMessage)

			if errorCount > handler.consumerGroupErrorConfig.MaxErrorCount {
				err := errors.New(getErrorMessage(consumerMessage))
				reachedMaxRetryErrorCountErr := fmt.Errorf("reached max error count, errRetriedCount: %d", errorCount)
				joinedErr := errors.Join(err, reachedMaxRetryErrorCountErr)
				log.Errorf("Reached max retry count, topic: %s, err: %s", handler.consumerGroupErrorConfig.Topics, joinedErr)
				session.MarkMessage(message, "")
				continue
			}
			if err := processMessage(ctx, consumer, consumerMessage, handler.consumerGroupErrorConfig.MaxProcessingTime); err != nil {
				log.Errorf("Reached max retry count, topic: %s, err: %s", handler.consumerGroupErrorConfig.Topics, err.Error())
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			state.Status = ConsumerGroupHandlerTopicClosed
			return nil
		}
	}
}

func (handler *errorConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	for _, state := range handler.state.ConsumerTopicStates {
		state.Status = ConsumerGroupHandlerTopicClosed
		state.ClosedDate = time.Now()
	}
	handler.state.Status = ConsumerGroupHandlerClosed
	handler.state.ClosedDate = time.Now()
	return nil
}
