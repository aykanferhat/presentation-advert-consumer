package kafka

import (
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"strings"
)

type Message interface {
	GetConfigName() string
	GetKey() string
}

type CustomMessage struct {
	Key   string
	Body  interface{}
	Topic *ProducerTopic
}

type ProducerTopicConfigMap map[string]*ProducerTopic

type ProducerTopic struct {
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
}

func (c ProducerTopicConfigMap) GetConfig(name string) (*ProducerTopic, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.Name) == 0 {
			return nil, custom_error.NewErrWithArgs("producer topic name required, config %s", name)
		}
		if len(config.Cluster) == 0 {
			return nil, custom_error.NewErrWithArgs("producer topic cluster required, config %s", name)
		}
		return config, nil
	}
	return nil, custom_error.NewErrWithArgs("producer topic config not found by name: %s", name)
}
