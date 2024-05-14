package elastic

import (
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"strings"
	"time"
)

type ConfigMap map[string]*Config

func (c ConfigMap) GetConfig(name string) (*Config, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		return config, nil
	}
	return nil, custom_error.NewConfigNotFoundErr(name)
}

type Config struct {
	Addresses             string        `json:"addresses"`
	MaxIdleConnPerHost    int           `json:"maxIdleConnPerHost"`
	MaxIdleConnDuration   time.Duration `json:"maxIdleConnDuration"`
	DiscoverNodesInterval time.Duration `json:"discoverNodesInterval"`
	DiscoverNodesOnStart  bool          `json:"discoverNodesOnStart"`
	ReadTimeout           time.Duration `json:"readTimeout"`
	WriteTimeout          time.Duration `json:"writeTimeout"`
}
