package kafka

type SyncProducer interface {
	SendMessage(message *ProducerMessage) (int32, int64, error)
	SendMessages(messages []*ProducerMessage) error
}
