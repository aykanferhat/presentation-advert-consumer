package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"time"
)

type Client = sarama.Client

func getSaramaConfig(clusterConfig *ClusterConfig, consumerTopicConfig *ConsumerGroupConfig) (*sarama.Config, error) {
	v, err := sarama.ParseKafkaVersion(clusterConfig.Version)
	if err != nil {
		return nil, err
	}
	if len(clusterConfig.ClientId) == 0 {
		return nil, custom_error.NewErr("clientId is empty in kafka config")
	}
	config := sarama.NewConfig()
	config.ChannelBufferSize = 256
	config.ApiVersionsRequest = true
	config.MetricRegistry = metrics.NewRegistry()
	config.Version = v
	config.ClientID = clusterConfig.ClientId

	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.Metadata.Full = false

	config.Net.ReadTimeout = 3 * time.Minute
	config.Net.DialTimeout = 3 * time.Minute
	config.Net.WriteTimeout = 3 * time.Minute

	// producer
	saramaRequiredAcks, err := clusterConfig.ProducerConfig.RequiredAcks.GetSaramaRequiredAcks()
	if err != nil {
		return nil, err
	}
	codec, err := clusterConfig.ProducerConfig.Compression.Codec()
	if err != nil {
		return nil, err
	}

	config.Producer.Retry.Max = 2
	config.Producer.Retry.Backoff = 1500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = saramaRequiredAcks
	config.Producer.Timeout = clusterConfig.ProducerConfig.Timeout
	config.Producer.MaxMessageBytes = clusterConfig.ProducerConfig.MaxMessageBytes
	config.Producer.Compression = codec

	// consumer
	if consumerTopicConfig != nil {
		offsetInitialIndex, err := consumerTopicConfig.OffsetInitial.GetSaramaOffsetInitialIndex()
		if err != nil {
			return nil, err
		}
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = offsetInitialIndex
		config.Consumer.Group.Session.Timeout = consumerTopicConfig.SessionTimeout
		config.Consumer.Group.Heartbeat.Interval = consumerTopicConfig.HeartbeatInterval
		config.Consumer.MaxProcessingTime = consumerTopicConfig.MaxProcessingTime
		config.Consumer.Fetch.Default = consumerTopicConfig.FetchMaxBytes
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		config.Consumer.Group.Rebalance.Timeout = consumerTopicConfig.RebalanceTimeout
		config.Consumer.Offsets.AutoCommit.Enable = true
		config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	} else {
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		config.Consumer.Offsets.AutoCommit.Enable = true
		config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
		return config, nil
	}
	return config, nil
}

func getSaramaErrorConfig(clusterConfig *ClusterConfig, consumerGroupConfig *ConsumerGroupErrorConfig) (*sarama.Config, error) {
	v, err := sarama.ParseKafkaVersion(clusterConfig.Version)
	if err != nil {
		return nil, err
	}
	if len(clusterConfig.ClientId) == 0 {
		return nil, custom_error.NewErr("clientId is empty in kafka config")
	}
	config := sarama.NewConfig()
	config.ChannelBufferSize = 256
	config.ApiVersionsRequest = true
	config.MetricRegistry = metrics.NewRegistry()
	config.Version = v
	config.ClientID = clusterConfig.ClientId

	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Second
	config.Metadata.Full = false

	config.Net.ReadTimeout = 3 * time.Minute
	config.Net.DialTimeout = 3 * time.Minute
	config.Net.WriteTimeout = 3 * time.Minute

	// producer
	saramaRequiredAcks, err := clusterConfig.ProducerConfig.RequiredAcks.GetSaramaRequiredAcks()
	if err != nil {
		return nil, err
	}

	config.Producer.Retry.Max = 2
	config.Producer.Retry.Backoff = 1500 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = saramaRequiredAcks
	config.Producer.Timeout = clusterConfig.ProducerConfig.Timeout
	config.Producer.MaxMessageBytes = clusterConfig.ProducerConfig.MaxMessageBytes

	// consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = consumerGroupConfig.SessionTimeout
	config.Consumer.Group.Heartbeat.Interval = consumerGroupConfig.HeartbeatInterval
	offsetInitialIndex, err := consumerGroupConfig.OffsetInitial.GetSaramaOffsetInitialIndex()
	if err != nil {
		return nil, err
	}
	config.Consumer.Offsets.Initial = offsetInitialIndex
	config.Consumer.MaxProcessingTime = consumerGroupConfig.MaxProcessingTime
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	return config, nil
}
