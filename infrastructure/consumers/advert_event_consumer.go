package consumers

import (
	"context"
	"presentation-advert-consumer/application/commands"
	"presentation-advert-consumer/application/handlers"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/kafka"
	"presentation-advert-consumer/infrastructure/consumers/model"
)

type advertEventConsumer struct {
	commandHandler *handlers.CommandHandler
}

func NewAdvertEventConsumer(commandHandler *handlers.CommandHandler) kafka.Consumer {
	return &advertEventConsumer{
		commandHandler: commandHandler,
	}
}

func (consumer *advertEventConsumer) Consume(ctx context.Context, msg *kafka.ConsumerMessage) error {
	var event model.AdvertEvent
	if err := custom_json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}
	return consumer.commandHandler.IndexAdvert.Handle(ctx, &commands.IndexAdvert{Id: event.Id})
}
