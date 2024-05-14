package kafka

import (
	"github.com/IBM/sarama"
	"github.com/robfig/cron/v3"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"time"
)

type ErrorConsumerGroup interface {
	ScheduleToSubscribe() error
	GetGroupId() string
	Subscribe()
	Unsubscribe() error
	IsSubscribed() bool
}

type errorConsumerGroup struct {
	client                               Client
	clusterConfig2                       *ClusterConfig
	consumerGroup                        sarama.ConsumerGroup
	errorConsumerGroupHandler            consumerGroupHandler
	consumerGroupErrorConfig             *ConsumerGroupErrorConfig
	errorTopicConsumerMap                map[string]Consumer
	scheduleToSubscribeCron              *cron.Cron
	checkConsumerGroupHandlerStateTicker *time.Ticker
	state                                *ConsumerGroupState
}

func NewErrorConsumerGroup(
	clusterConfig *ClusterConfig,
	consumerGroupErrorConfig *ConsumerGroupErrorConfig,
	errorTopicConsumerMap map[string]Consumer,
) (ErrorConsumerGroup, error) {
	errorConsumerGroup := &errorConsumerGroup{
		clusterConfig2:                       clusterConfig,
		consumerGroupErrorConfig:             consumerGroupErrorConfig,
		errorTopicConsumerMap:                errorTopicConsumerMap,
		scheduleToSubscribeCron:              cron.New(),
		checkConsumerGroupHandlerStateTicker: time.NewTicker(2 * time.Second),
		state: &ConsumerGroupState{
			Status: ConsumerGroupCreated,
		},
	}
	go func() {
		errorConsumerGroup.listenConsumerStatus()
	}()
	return errorConsumerGroup, nil
}

func (c *errorConsumerGroup) ScheduleToSubscribe() error {
	if _, err := c.scheduleToSubscribeCron.AddFunc(c.consumerGroupErrorConfig.Cron, c.Subscribe); err != nil {
		return err
	}
	c.scheduleToSubscribeCron.Start()
	return nil
}

func (c *errorConsumerGroup) IsStarted() bool {
	if !c.IsSubscribed() {
		return false
	}
	if c.state.ConsumerGroupHandlerState.Status != ConsumerGroupHandlerStarted {
		return false
	}
	for _, state := range c.state.ConsumerGroupHandlerState.ConsumerTopicStates {
		if state.Status != ConsumerGroupHandlerTopicListening && state.Status != ConsumerGroupHandlerTopicStarted {
			return false
		}
	}
	return true
}

func (c *errorConsumerGroup) GetGroupId() string {
	return c.consumerGroupErrorConfig.GroupId
}

func (c *errorConsumerGroup) Subscribe() {
	if c.IsSubscribed() {
		log.Infof("errorConsumerGroup is already running, groupId: %s", c.consumerGroupErrorConfig.GroupId)
		return
	}
	topics := c.getErrorTopics()
	if len(topics) == 0 {
		return
	}
	log.Infof("errorConsumerGroup Subscribe, groupId: %s", c.consumerGroupErrorConfig.GroupId)
	saramaConfig, err := getSaramaErrorConfig(c.clusterConfig2, c.consumerGroupErrorConfig)
	if err != nil {
		log.Errorf("errorConsumerGroup Subscribe err: %s", err.Error())
		return
	}
	handler := newErrorConsumerGroupHandler(c.consumerGroupErrorConfig, c.errorTopicConsumerMap)
	c.errorConsumerGroupHandler = handler
	c.state.ConsumerGroupHandlerState = handler.GetState()

	client, cg, err := subscribe(saramaConfig, c.clusterConfig2, c.errorConsumerGroupHandler, c.consumerGroupErrorConfig.GroupId, topics, true)
	if err != nil {
		log.Errorf("errorConsumerGroup Subscribe err: %s", err.Error())
		return
	}
	c.client = client
	c.consumerGroup = cg
	c.state.Status = ConsumerGroupSubscribed
}

func (c *errorConsumerGroup) Unsubscribe() error {
	if !c.existsErrorTopic() {
		return nil
	}
	log.Infof("errorConsumerGroup Unsubscribe, groupId: %s", c.consumerGroupErrorConfig.GroupId)
	if err := unsubscribe(c.client, c.consumerGroup); err != nil {
		log.Errorf("errorConsumerGroup Unsubscribe err: %s", err.Error())
		return err
	}
	c.client = nil
	c.consumerGroup = nil
	c.state.Status = ConsumerGroupUnsubscribed
	return nil
}

func (c *errorConsumerGroup) listenConsumerStatus() {
	closeConsumerWhenThereIsNoMessage := c.consumerGroupErrorConfig.CloseConsumerWhenThereIsNoMessage.Nanoseconds()
	closeConsumerWhenMessageIsNew := c.consumerGroupErrorConfig.CloseConsumerWhenMessageIsNew.Nanoseconds()
	for range c.checkConsumerGroupHandlerStateTicker.C {
		if c.state.Status != ConsumerGroupSubscribed {
			continue
		}
		if c.state.ConsumerGroupHandlerState == nil {
			continue
		}
		if c.state.ConsumerGroupHandlerState.Status == ConsumerGroupHandlerClosed {
			_ = c.Unsubscribe()
			continue
		}
		allTopicStateAreUnsubscribable := false
		for _, state := range c.state.ConsumerGroupHandlerState.ConsumerTopicStates {
			if state.Status == ConsumerGroupHandlerTopicNewMessage ||
				(state.Status == ConsumerGroupHandlerTopicCreated && time.Since(state.CreatedDate).Nanoseconds() > closeConsumerWhenThereIsNoMessage) ||
				(state.Status == ConsumerGroupHandlerTopicListening && time.Since(state.ListeningDate).Nanoseconds() > closeConsumerWhenThereIsNoMessage) ||
				(state.Status == ConsumerGroupHandlerTopicStarted && time.Since(state.LatestConsumedDate).Nanoseconds() > closeConsumerWhenThereIsNoMessage) ||
				(state.Status == ConsumerGroupHandlerTopicStarted && time.Since(state.LatestConsumedDate).Nanoseconds() > closeConsumerWhenMessageIsNew) {
				allTopicStateAreUnsubscribable = true
			} else {
				allTopicStateAreUnsubscribable = false
				break
			}
		}
		if allTopicStateAreUnsubscribable {
			_ = c.Unsubscribe()
		}
	}
}

func (c *errorConsumerGroup) IsSubscribed() bool {
	return c.state.Status == ConsumerGroupSubscribed
}

func (c *errorConsumerGroup) existsErrorTopic() bool {
	return len(c.errorTopicConsumerMap) != 0
}

func (c *errorConsumerGroup) getErrorTopics() map[string]struct{} {
	topics := make(map[string]struct{})
	for topic := range c.errorTopicConsumerMap {
		topics[topic] = struct{}{}
	}
	return topics
}
