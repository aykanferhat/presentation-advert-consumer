package kafka

type consumerBuilder struct {
	clusterConfigMap  ClusterConfigMap
	consumerConfigMap ConsumerGroupConfigMap
	consumersList     []*ConsumerGroupConsumers
}

func NewConsumerBuilder(
	clusterConfigMap ClusterConfigMap,
	consumerConfigMap ConsumerGroupConfigMap,
	consumersList []*ConsumerGroupConsumers,
) *consumerBuilder {
	return &consumerBuilder{
		clusterConfigMap:  clusterConfigMap,
		consumerConfigMap: consumerConfigMap,
		consumersList:     consumersList,
	}
}

func (c *consumerBuilder) Initialize() (map[string]ConsumerGroup, map[string]ErrorConsumerGroup, error) {
	producers, err := NewProducerBuilder(c.clusterConfigMap).Initialize()
	if err != nil {
		return nil, nil, err
	}
	clusterConsumerConfigConsumersMap := make(map[string]map[*ConsumerGroupConfig]*ConsumerGroupConsumers)
	consumerGroupMap := make(map[string]ConsumerGroup)

	for _, consumers := range c.consumersList {
		consumerGroupConfig, err := c.consumerConfigMap.GetConfigWithDefault(consumers.ConfigName)
		if err != nil {
			return nil, nil, err
		}
		clusterName := consumerGroupConfig.Cluster
		clusterConfig, err := c.clusterConfigMap.GetConfigWithDefault(clusterName)
		if err != nil {
			return nil, nil, err
		}
		producer, err := producers.GetSyncProducer(clusterName)
		if err != nil {
			return nil, nil, err
		}
		consumerGroup, err := NewConsumerGroup(clusterConfig, consumerGroupConfig, producer, consumers)
		if err != nil {
			return nil, nil, err
		}
		if err := consumerGroup.Subscribe(); err != nil {
			return nil, nil, err
		}
		if _, exist := clusterConsumerConfigConsumersMap[clusterName]; !exist {
			clusterConsumerConfigConsumersMap[clusterName] = make(map[*ConsumerGroupConfig]*ConsumerGroupConsumers)
		}
		clusterConsumerConfigConsumersMap[clusterName][consumerGroupConfig] = consumers
		consumerGroupMap[consumerGroup.GetGroupId()] = consumerGroup
	}
	errorConsumerGroupMap := make(map[string]ErrorConsumerGroup)
	for clusterName := range c.clusterConfigMap {
		clusterConfig, err := c.clusterConfigMap.GetConfigWithDefault(clusterName)
		if err != nil {
			return nil, nil, err
		}
		if clusterConfig.ErrorConfig == nil {
			continue
		}
		consumerConfigConsumersMap, exists := clusterConsumerConfigConsumersMap[clusterName]
		if !exists {
			continue
		}
		producer, err := producers.GetSyncProducer(clusterName)
		if err != nil {
			return nil, nil, err
		}
		var topics []string
		errorTopicConsumerMap := make(map[string]Consumer)
		for config, consumers := range consumerConfigConsumersMap {
			if config.IsDisabledErrorConsumer() || len(config.Error) == 0 {
				continue
			}
			if consumers.ErrorConsumer != nil {
				errorTopicConsumerMap[config.Error] = consumers.ErrorConsumer
			} else {
				errorTopicConsumerMap[config.Error] = NewDefaultErrorConsumer(producer)
			}
			topics = append(topics, config.Error)
		}
		consumerGroupErrorConfig := &ConsumerGroupErrorConfig{
			GroupId:                           clusterConfig.ErrorConfig.GroupId,
			Topics:                            topics,
			Cron:                              clusterConfig.ErrorConfig.Cron,
			MaxErrorCount:                     clusterConfig.ErrorConfig.MaxErrorCount,
			Cluster:                           clusterName,
			CloseConsumerWhenThereIsNoMessage: clusterConfig.ErrorConfig.CloseConsumerWhenThereIsNoMessage,
			CloseConsumerWhenMessageIsNew:     clusterConfig.ErrorConfig.CloseConsumerWhenMessageIsNew,
			MaxProcessingTime:                 clusterConfig.ErrorConfig.MaxProcessingTime,
			FetchMaxBytes:                     clusterConfig.ErrorConfig.FetchMaxBytes,
			OffsetInitial:                     clusterConfig.ErrorConfig.OffsetInitial,
			SessionTimeout:                    clusterConfig.ErrorConfig.SessionTimeout,
			RebalanceTimeout:                  clusterConfig.ErrorConfig.RebalanceTimeout,
			HeartbeatInterval:                 clusterConfig.ErrorConfig.HeartbeatInterval,
		}
		errorConsumer, err := NewErrorConsumerGroup(clusterConfig, consumerGroupErrorConfig, errorTopicConsumerMap)
		if err != nil {
			return nil, nil, err
		}
		if err := errorConsumer.ScheduleToSubscribe(); err != nil {
			return nil, nil, err
		}
		errorConsumerGroupMap[consumerGroupErrorConfig.GroupId] = errorConsumer
	}
	return consumerGroupMap, errorConsumerGroupMap, nil
}
