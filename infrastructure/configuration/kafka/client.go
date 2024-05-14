package kafka

import (
	"github.com/IBM/sarama"
)

func NewClient(saramaConfig *sarama.Config, clusterConfig *ClusterConfig) (Client, error) {
	brokers := clusterConfig.GetBrokers()
	return sarama.NewClient(brokers, saramaConfig)
}

func NewProducerClient(clusterConfig *ClusterConfig) (Client, error) {
	kafkaConfig, err := getSaramaConfig(clusterConfig, nil)
	if err != nil {
		return nil, err
	}
	brokers := clusterConfig.GetBrokers()
	return sarama.NewClient(brokers, kafkaConfig)
}
