package client_config

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
	Url                string        `json:"url"`
	MaxConnections     int           `json:"maxConnections"`
	MaxIdleConDuration time.Duration `json:"maxIdleConDuration"`
	ReadTimeout        time.Duration `json:"readTimeout"`
	WriteTimeout       time.Duration `json:"writeTimeout"`
}
