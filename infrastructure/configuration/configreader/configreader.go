package configreader

import (
	"github.com/spf13/viper"
	"os"
	"presentation-advert-consumer/infrastructure/configuration/client_config"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/infrastructure/configuration/kafka"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"presentation-advert-consumer/infrastructure/configuration/server"
)

const (
	configPath     = "./configs"
	yamlConfigType = "yaml"
)

func ReadServerConf(serverConfigPath string) *server.Config {
	var conf server.Config
	err := readFile(&conf, serverConfigPath)
	if err != nil {
		log.Panic("Server Config file couldn't read")
	}
	return &conf
}

func ReadClientConfig(clientConfigPath string) client_config.ConfigMap {
	var conf map[string]*client_config.Config
	err := readFile(&conf, clientConfigPath)
	if err != nil {
		log.Panic("Client Config file couldn't read")
	}
	return conf
}

func ReadLogConfig(logConfigPath string) *log.Config {
	var conf log.Config
	err := readFile(&conf, logConfigPath)
	if err != nil {
		log.Panic("Log Config file couldn't read")
	}
	return &conf
}

func ReadElasticConfig(elasticConfigPath string) elastic.ConfigMap {
	var conf map[string]*elastic.Config
	err := readFile(&conf, elasticConfigPath)
	if err != nil {
		log.Panic("Elastic Config file couldn't read")
	}
	return conf
}

func GetProfile(envName string, defaultValue string) string {
	profile := os.Getenv(envName)
	if profile == "" {
		profile = defaultValue
	}
	return profile
}

func ReadKafkaClusterConfig(kafkaConfigPath string) kafka.ClusterConfigMap {
	var conf map[string]*kafka.ClusterConfig
	if err := readFile(&conf, kafkaConfigPath); err != nil {
		log.Panic("Kafka Cluster Config file couldn't read")
	}
	return conf
}

func ReadKafkaConsumerGroupConfig(kafkaConsumerConfigPath string) kafka.ConsumerGroupConfigMap {
	var conf map[string]*kafka.ConsumerGroupConfig
	if err := readFile(&conf, kafkaConsumerConfigPath); err != nil {
		log.Panic("Kafka Cluster Config file couldn't read")
	}
	return conf
}

func ReadKafkaConsumerGroupErrorConfig(kafkaConsumerErrorConfigPath string) kafka.ConsumerGroupErrorConfigMap {
	var conf map[string]*kafka.ConsumerGroupErrorConfig
	if err := readFile(&conf, kafkaConsumerErrorConfigPath); err != nil {
		log.Panic("Kafka Consumer Group Config file couldn't read")
	}
	return conf
}

func ReadKafkaProducerTopicConfig(kafkaProducerConfigPath string) kafka.ProducerTopicConfigMap {
	var conf map[string]*kafka.ProducerTopic
	if err := readFile(&conf, kafkaProducerConfigPath); err != nil {
		log.Panic("Kafka Producer Topic Config file couldn't read")
	}
	return conf
}

func readFile(conf interface{}, filePath string) error {
	viper.AddConfigPath(configPath)
	viper.SetConfigType(yamlConfigType)
	viper.SetConfigName(filePath)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(&conf); err != nil {
		return err
	}
	return nil
}
