package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"strings"
)

type ProducerMessage = sarama.ProducerMessage
type ProducerError = sarama.ProducerError

type Producer interface {
	GetSyncProducer(clusterName string) (SyncProducer, error)
	GetProducerTopic(configName string) (*ProducerTopic, error)
	ProduceSync(ctx context.Context, message Message) error
	ProduceSyncBulk(ctx context.Context, messages []Message, size int) error
	ProduceCustomSync(ctx context.Context, message *CustomMessage) error
	ProduceCustomSyncBulk(ctx context.Context, messages []*CustomMessage, size int) error
}

type producer struct {
	syncProducerMap        map[string]SyncProducer
	producerTopicConfigMap ProducerTopicConfigMap
}

func NewProducer(
	syncProducerMap map[string]SyncProducer,
	producerTopicConfigMap ProducerTopicConfigMap,
) (Producer, error) {
	return &producer{
		syncProducerMap:        syncProducerMap,
		producerTopicConfigMap: producerTopicConfigMap,
	}, nil
}

type producerMessage struct {
	Message         Message
	ProducerMessage *ProducerMessage
	Topic           *ProducerTopic
}

func (c *producer) GetSyncProducer(clusterName string) (SyncProducer, error) {
	producer, exist := c.syncProducerMap[strings.ToLower(clusterName)]
	if !exist {
		return nil, custom_error.NewErrWithArgs("kafka sync producer not found. cluster name: %s", clusterName)
	}
	return producer, nil
}

func (c *producer) GetProducerTopic(configName string) (*ProducerTopic, error) {
	return c.producerTopicConfigMap.GetConfig(configName)
}

func (c *producer) ProduceSync(_ context.Context, message Message) error {
	produceMessage, err := c.getProduceMessageFromMessage(message)
	if err != nil {
		return err
	}
	producer, err := c.GetSyncProducer(produceMessage.Topic.Cluster)
	if err != nil {
		return err
	}
	if _, _, err = producer.SendMessage(produceMessage.ProducerMessage); err != nil {
		return errors.Join(err, fmt.Errorf("produce sync err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic.Name, produceMessage.Topic.Cluster))
	}
	return nil
}

func (c *producer) ProduceSyncBulk(_ context.Context, messages []Message, size int) error {
	mappedProducerMessages := make([]*producerMessage, 0, len(messages))
	for _, message := range messages {
		produceMessage, err := c.getProduceMessageFromMessage(message)
		if err != nil {
			return err
		}
		mappedProducerMessages = append(mappedProducerMessages, produceMessage)
	}
	slicedProducerMessages, err := c.splitAndSliceKafKaMessages(mappedProducerMessages, size)
	if err != nil {
		return err
	}
	for _, producerMessages := range slicedProducerMessages {
		topic := producerMessages[0].Topic.Name
		clusterName := producerMessages[0].Topic.Cluster
		syncProducer, err := c.GetSyncProducer(clusterName)
		if err != nil {
			return err
		}
		saramaMessages := make([]*ProducerMessage, 0, len(producerMessages))
		for _, producerMessage := range producerMessages {
			saramaMessages = append(saramaMessages, producerMessage.ProducerMessage)
		}
		if err = syncProducer.SendMessages(saramaMessages); err != nil {
			return errors.Join(err, fmt.Errorf("produce sync bulk err: %s, topic: %s, cluster: %s", err.Error(), topic, clusterName))
		}
	}
	return nil
}

func (c *producer) ProduceCustomSync(_ context.Context, message *CustomMessage) error {
	produceMessage, err := c.getProduceMessageFromCustomMessage(message)
	if err != nil {
		return err
	}
	syncProducer, err := c.GetSyncProducer(message.Topic.Cluster)
	if err != nil {
		return err
	}
	if _, _, err = syncProducer.SendMessage(produceMessage.ProducerMessage); err != nil {
		return errors.Join(err, fmt.Errorf("produce custom sync err: %s, topic: %s, cluster: %s", err.Error(), produceMessage.Topic.Name, produceMessage.Topic.Cluster))
	}
	return nil
}

func (c *producer) ProduceCustomSyncBulk(_ context.Context, messages []*CustomMessage, size int) error {
	mappedProducerMessages := make([]*producerMessage, 0, len(messages))
	for _, message := range messages {
		saramaProduceMessage, err := c.getProduceMessageFromCustomMessage(message)
		if err != nil {
			return err
		}
		mappedProducerMessages = append(mappedProducerMessages, saramaProduceMessage)
	}
	slicedProducerMessages, err := c.splitAndSliceKafKaMessages(mappedProducerMessages, size)
	if err != nil {
		return err
	}
	for _, producerMessages := range slicedProducerMessages {
		topic := producerMessages[0].Topic.Name
		clusterName := producerMessages[0].Topic.Cluster
		syncProducer, err := c.GetSyncProducer(clusterName)
		if err != nil {
			return err
		}
		saramaMessages := make([]*ProducerMessage, 0, len(producerMessages))
		for _, producerMessage := range producerMessages {
			saramaMessages = append(saramaMessages, producerMessage.ProducerMessage)
		}
		if err = syncProducer.SendMessages(saramaMessages); err != nil {
			return errors.Join(err, fmt.Errorf("produce custom sync bulk err: %s, topic: %s, cluster: %s", err.Error(), topic, clusterName))
		}
	}
	return nil
}

func (c *producer) getProduceMessageFromCustomMessage(message *CustomMessage) (*producerMessage, error) {
	saramaProduceMessage, err := c.mapToProducerMessage(message.Topic.Name, message.Key, message.Body)
	if err != nil {
		return nil, err
	}
	return &producerMessage{
		Message:         nil,
		ProducerMessage: saramaProduceMessage,
		Topic:           message.Topic,
	}, nil
}

func (c *producer) getProduceMessageFromMessage(message Message) (*producerMessage, error) {
	producerTopic, err := c.GetProducerTopic(message.GetConfigName())
	if err != nil {
		return nil, err
	}
	saramaProducerMessage, err := c.mapToProducerMessage(producerTopic.Name, message.GetKey(), message)
	if err != nil {
		return nil, err
	}
	return &producerMessage{
		Message:         message,
		Topic:           producerTopic,
		ProducerMessage: saramaProducerMessage,
	}, nil
}

func (c *producer) mapToProducerMessage(topic string, key string, message interface{}) (*ProducerMessage, error) {
	body, err := custom_json.Marshal(message)
	if err != nil {
		return nil, err
	}
	var producerMessage *ProducerMessage
	if key == "" {
		producerMessage = &ProducerMessage{
			Value: sarama.StringEncoder(body),
			Topic: topic,
		}
	} else {
		producerMessage = &ProducerMessage{
			Value: sarama.StringEncoder(body),
			Key:   sarama.StringEncoder(key),
			Topic: topic,
		}
	}
	return producerMessage, nil
}

func (c *producer) splitAndSliceKafKaMessages(kafkaMessages []*producerMessage, arraySize int) ([][]*producerMessage, error) {
	clusterMessageMap := make(map[string][]*producerMessage)
	for _, m := range kafkaMessages {
		clusterName := m.Topic.Cluster
		if _, exist := clusterMessageMap[clusterName]; !exist {
			clusterMessageMap[clusterName] = make([]*producerMessage, 0)
		}
		clusterMessageMap[clusterName] = append(clusterMessageMap[clusterName], m)
	}
	slicedMessages := make([][]*producerMessage, 0)
	for _, messages := range clusterMessageMap {
		slicedMessages = append(slicedMessages, c.sliceKafkaMessages(messages, arraySize)...)
	}
	return slicedMessages, nil
}

func (c *producer) sliceKafkaMessages(producerMessages []*producerMessage, arraySize int) [][]*producerMessage {
	tempList := make([][]*producerMessage, 0)
	if len(producerMessages) == arraySize {
		return append(tempList, producerMessages)
	}
	var mod = len(producerMessages) % arraySize
	for i := 0; i <= len(producerMessages)-arraySize; i = i + arraySize {
		var arrayPart = producerMessages[i : i+arraySize]
		tempList = append(tempList, arrayPart)
	}
	var last = len(producerMessages) - mod
	var arrayPart = producerMessages[last:]
	if len(arrayPart) != 0 {
		tempList = append(tempList, arrayPart)
	}
	return tempList
}
