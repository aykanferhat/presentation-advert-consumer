package kafka

import (
	"github.com/IBM/sarama"
)

type producerBuilder struct {
	clusterConfigMap ClusterConfigMap
	successHandler   func(message *ProducerMessage)
	errorHandler     func(err *ProducerError)
	topicConfigMap   ProducerTopicConfigMap
}

func NewProducerBuilder(clusterConfigMap ClusterConfigMap) *producerBuilder {
	return NewProducerBuilderWithConfig(clusterConfigMap, make(ProducerTopicConfigMap))
}

func NewProducerBuilderWithConfig(clusterConfigMap ClusterConfigMap, topicConfigMap ProducerTopicConfigMap) *producerBuilder {
	return &producerBuilder{
		clusterConfigMap: clusterConfigMap,
		successHandler: func(message *ProducerMessage) {
			// do nothing
		},
		errorHandler: func(err *ProducerError) {
			// do nothing
		},
		topicConfigMap: topicConfigMap,
	}
}

func (p *producerBuilder) Initialize() (Producer, error) {
	syncProducerMap := make(map[string]SyncProducer)
	for cluster := range p.clusterConfigMap {
		clusterConfig, err := p.clusterConfigMap.GetConfigWithDefault(cluster)
		if err != nil {
			return nil, err
		}
		client, err := NewProducerClient(clusterConfig)
		if err != nil {
			return nil, err
		}
		syncProducer, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			return nil, err
		}
		syncProducerMap[cluster] = syncProducer
	}
	return NewProducer(syncProducerMap, p.topicConfigMap)
}
