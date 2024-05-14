package kafka

import (
	"github.com/IBM/sarama"
	"github.com/docker/go-units"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"strings"
	"time"
)

type ConsumerGroupConfig struct {
	GroupId              string        `json:"groupId"`
	Name                 string        `json:"name"`
	Retry                string        `json:"retry"`
	Error                string        `json:"error"`
	RetryCount           int           `json:"retryCount"`
	Cluster              string        `json:"cluster"`
	MaxProcessingTime    time.Duration `json:"maxProcessingTime"`
	FetchMaxBytes        int32         `json:"fetchMaxBytes"`
	DisableErrorConsumer bool          `json:"disableErrorConsumer"`
	OffsetInitial        OffsetInitial `json:"offsetInitial"`
	SessionTimeout       time.Duration `json:"sessionTimeout"`
	RebalanceTimeout     time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval    time.Duration `json:"heartbeatInterval"`
}

func (c *ConsumerGroupConfig) GetTopics() map[string]struct{} {
	topics := make(map[string]struct{})
	topics[c.Name] = struct{}{}
	if len(c.Retry) != 0 {
		topics[c.Retry] = struct{}{}
	}
	return topics
}

func (c *ConsumerGroupConfig) IsNotDefinedRetryAndErrorTopic() bool {
	return c.IsNotDefinedRetryTopic() && c.IsNotDefinedErrorTopic()
}

func (c *ConsumerGroupConfig) IsNotDefinedRetryTopic() bool {
	return len(c.Retry) == 0
}

func (c *ConsumerGroupConfig) IsNotDefinedErrorTopic() bool {
	return len(c.Error) == 0
}

func (c *ConsumerGroupConfig) IsDefinedErrorTopic() bool {
	return len(c.Error) > 0
}

func (c *ConsumerGroupConfig) IsDisabledErrorConsumer() bool {
	return c.DisableErrorConsumer || len(c.Error) == 0
}

type ConsumerGroupConfigMap map[string]*ConsumerGroupConfig

func (c ConsumerGroupConfigMap) GetConfigWithDefault(name string) (*ConsumerGroupConfig, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.GroupId) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer topic config group id required, config name: %s", name)
		}
		if len(config.Name) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer topic config topic name required, config name: %s", name)
		}
		if config.RetryCount > 0 && len(config.Retry) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer topic config retry topic name required, config name: %s", name)
		}
		if len(config.Retry) > 0 && config.RetryCount == 0 {
			config.RetryCount = 1
		}
		if config.FetchMaxBytes == 0 {
			config.FetchMaxBytes = 1 * units.MiB
		}
		if config.MaxProcessingTime == 0 {
			config.MaxProcessingTime = 1 * time.Minute
		}
		if len(config.OffsetInitial) == 0 {
			config.OffsetInitial = OffsetNewest
		}
		if config.SessionTimeout == 0 {
			config.SessionTimeout = 10 * time.Second
		}
		if config.RebalanceTimeout == 0 {
			config.RebalanceTimeout = 60 * time.Second
		}
		if config.HeartbeatInterval == 0 {
			config.HeartbeatInterval = 3 * time.Second
		}
		return config, nil
	}
	return nil, custom_error.NewErrWithArgs("config not found: %s", name)
}

type OffsetInitial string

const (
	OffsetNewest OffsetInitial = "newest"
	OffsetOldest OffsetInitial = "oldest"
)

func (i OffsetInitial) GetSaramaOffsetInitialIndex() (int64, error) {
	switch i {
	case OffsetNewest:
		return sarama.OffsetNewest, nil
	case OffsetOldest:
		return sarama.OffsetOldest, nil
	default:
		return 0, custom_error.NewErrWithArgs("OffsetInitial value not match, it should be OffsetNewest or OffsetOldest")
	}
}
