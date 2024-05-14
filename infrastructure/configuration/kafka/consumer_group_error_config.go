package kafka

import (
	"github.com/docker/go-units"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"strings"
	"time"
)

type ConsumerGroupErrorConfig struct {
	GroupId                           string        `json:"groupId"`
	Topics                            []string      `json:"topics"`
	TargetTopic                       string        `json:"targetTopic"`
	Cron                              string        `json:"cron"`
	MaxErrorCount                     int           `json:"maxErrorCount"`
	Cluster                           string        `json:"cluster"`
	CloseConsumerWhenThereIsNoMessage time.Duration `json:"closeConsumerWhenThereIsNoMessage"`
	CloseConsumerWhenMessageIsNew     time.Duration `json:"closeConsumerWhenMessageIsNew"`
	MaxProcessingTime                 time.Duration `json:"maxProcessingTime"`
	FetchMaxBytes                     int32         `json:"fetchMaxBytes"`
	OffsetInitial                     OffsetInitial `json:"offsetInitial"`
	SessionTimeout                    time.Duration `json:"sessionTimeout"`
	RebalanceTimeout                  time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval                 time.Duration `json:"heartbeatInterval"`
}

type ConsumerGroupErrorConfigMap map[string]*ConsumerGroupErrorConfig

func (c ConsumerGroupErrorConfigMap) GetConfigWithDefault(name string) (*ConsumerGroupErrorConfig, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.GroupId) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer error config group id required, config name: %s", name)
		}
		if len(config.Topics) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer error config name required, config name: %s", name)
		}
		if len(config.Cron) == 0 {
			return nil, custom_error.NewErrWithArgs("consumer error config cron required, config name: %s", name)
		}
		if config.MaxErrorCount == 0 {
			return nil, custom_error.NewErrWithArgs("consumer error config max error, config name: %s", name)
		}
		if config.CloseConsumerWhenThereIsNoMessage == 0 {
			config.CloseConsumerWhenThereIsNoMessage = 1 * time.Minute
		}
		if config.CloseConsumerWhenMessageIsNew == 0 {
			config.CloseConsumerWhenMessageIsNew = 1 * time.Minute
		}
		if config.FetchMaxBytes == 0 {
			config.FetchMaxBytes = 1 * units.MiB
		}
		if config.MaxProcessingTime == 0 {
			config.MaxProcessingTime = 1 * time.Second
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
	return nil, custom_error.NewErrWithArgs("error consumer config not found: %s", name)
}
