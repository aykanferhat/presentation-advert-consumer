package kafka

import (
	"github.com/IBM/sarama"
)

type ConsumerGroup interface {
	GetGroupId() string
	Subscribe() error
	Unsubscribe() error
}

type consumerGroup struct {
	clusterConfig *ClusterConfig
	topicConfig   *ConsumerGroupConfig

	// create in NewConsumerGroup
	consumerGroupHandler consumerGroupHandler
	// create in Subscribe
	client        Client
	consumerGroup sarama.ConsumerGroup
}

func NewConsumerGroup(
	clusterConfig *ClusterConfig,
	consumerGroupConfig *ConsumerGroupConfig,
	producer SyncProducer,
	consumers *ConsumerGroupConsumers,
) (ConsumerGroup, error) {
	consumerGroupHandler := newConsumerGroupHandlerImpl(consumerGroupConfig, consumers, producer)
	return &consumerGroup{
		clusterConfig:        clusterConfig,
		topicConfig:          consumerGroupConfig,
		consumerGroupHandler: consumerGroupHandler,
	}, nil
}

func (c *consumerGroup) GetGroupId() string {
	return c.topicConfig.GroupId
}

func (c *consumerGroup) Subscribe() error {
	saramaConfig, err := getSaramaConfig(c.clusterConfig, c.topicConfig)
	if err != nil {
		return err
	}
	client, cg, err := subscribe(saramaConfig, c.clusterConfig, c.consumerGroupHandler, c.topicConfig.GroupId, c.topicConfig.GetTopics(), false)
	if err != nil {
		return err
	}
	c.client = client
	c.consumerGroup = cg
	return nil
}

func (c *consumerGroup) Unsubscribe() error {
	if err := unsubscribe(c.client, c.consumerGroup); err != nil {
		return err
	}
	c.client = nil
	c.consumerGroup = nil
	return nil
}
