package kafka

import (
	"github.com/IBM/sarama"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"strings"
	"time"
)

type ClusterConfig struct {
	Brokers        string          `json:"brokers"`
	Version        string          `json:"version"`
	ProducerConfig *ProducerConfig `json:"producerConfig"`
	ErrorConfig    *ErrorConfig    `json:"errorConfig"`
	ClientId       string          `json:"clientId"`
}

func (config *ClusterConfig) GetBrokers() []string {
	return strings.Split(strings.ReplaceAll(config.Brokers, " ", ""), ",")
}

type ProducerConfig struct {
	RequiredAcks    RequiredAcks  `json:"requiredAcks"`
	Timeout         time.Duration `json:"timeout"`
	MaxMessageBytes int           `json:"maxMessageBytes"`
	Compression     Compression   `json:"compression"`
}

type ClusterConfigMap map[string]*ClusterConfig

func (c ClusterConfigMap) GetConfigWithDefault(name string) (*ClusterConfig, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		if len(config.Brokers) == 0 {
			return nil, custom_error.NewErrWithArgs("brokers required, cluster: %s", name)
		}
		if len(config.Version) == 0 {
			return nil, custom_error.NewErrWithArgs("version required, cluster: %s", name)
		}
		if config.ProducerConfig != nil {
			if len(config.ProducerConfig.RequiredAcks) == 0 {
				config.ProducerConfig.RequiredAcks = WaitForLocal
			}
			if config.ProducerConfig.Timeout == 0 {
				config.ProducerConfig.Timeout = 10 * time.Second
			}
			if config.ProducerConfig.MaxMessageBytes == 0 {
				config.ProducerConfig.MaxMessageBytes = 1000000
			}
			if config.ProducerConfig.Compression == "" {
				config.ProducerConfig.Compression = CompressionNone
			}
		} else {
			config.ProducerConfig = &ProducerConfig{
				RequiredAcks:    WaitForLocal,
				Timeout:         10 * time.Second,
				MaxMessageBytes: 1000000,
				Compression:     CompressionNone,
			}
		}
		return config, nil
	}
	return nil, custom_error.NewErrWithArgs("cluster config not found: %s", name)
}

type ErrorConfig struct {
	GroupId                           string        `json:"groupId"`
	Cron                              string        `json:"cron"`
	MaxErrorCount                     int           `json:"maxErrorCount"`
	Tracer                            string        `json:"tracer"`
	MaxProcessingTime                 time.Duration `json:"maxProcessingTime"`
	CloseConsumerWhenThereIsNoMessage time.Duration `json:"closeConsumerWhenThereIsNoMessage"`
	CloseConsumerWhenMessageIsNew     time.Duration `json:"closeConsumerWhenMessageIsNew"`
	OffsetInitial                     OffsetInitial `json:"offsetInitial"`
	FetchMaxBytes                     int32         `json:"fetchMaxBytes"`
	SessionTimeout                    time.Duration `json:"sessionTimeout"`
	RebalanceTimeout                  time.Duration `json:"rebalanceTimeout"`
	HeartbeatInterval                 time.Duration `json:"heartbeatInterval"`
}

type RequiredAcks string

const (
	NoResponse   RequiredAcks = "NoResponse"
	WaitForLocal RequiredAcks = "WaitForLocal"
	WaitForAll   RequiredAcks = "WaitForAll"
)

func (i RequiredAcks) GetSaramaRequiredAcks() (sarama.RequiredAcks, error) {
	switch i {
	case NoResponse:
		return sarama.NoResponse, nil
	case WaitForLocal:
		return sarama.WaitForLocal, nil
	case WaitForAll:
		return sarama.WaitForAll, nil
	default:
		return 0, custom_error.NewErrWithArgs("RequiredAcks value not match, it should be NoResponse, WaitForLocal or WaitForAll")
	}
}

type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionGZIP   Compression = "gzip"
	CompressionSnappy Compression = "snappy"
	CompressionLZ4    Compression = "lz4"
	CompressionZSTD   Compression = "zstd"
)

func (c Compression) Codec() (sarama.CompressionCodec, error) {
	codecs := map[Compression]sarama.CompressionCodec{
		CompressionNone:   sarama.CompressionNone,
		CompressionGZIP:   sarama.CompressionGZIP,
		CompressionSnappy: sarama.CompressionSnappy,
		CompressionLZ4:    sarama.CompressionLZ4,
		CompressionZSTD:   sarama.CompressionZSTD,
	}
	codec, ok := codecs[c]
	if !ok {
		return 0, custom_error.NewErrWithArgs("Compression type not found, it should be none, gzip, snappy, lz4 or zstd")
	}
	return codec, nil
}
