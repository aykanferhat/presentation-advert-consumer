package kafka

type ConsumerGroupConsumers struct {
	ConfigName    string
	Consumer      Consumer
	ErrorConsumer Consumer
}

type ConsumerGroupErrorConsumers struct {
	ConfigName    string
	ErrorConsumer Consumer
}
