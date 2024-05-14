package consumers

import (
	"context"
	"presentation-advert-consumer/application/cacheservice"
	"presentation-advert-consumer/application/commands"
	"presentation-advert-consumer/application/handlers"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/kafka"
	"presentation-advert-consumer/infrastructure/consumers/model"
)

type categoryEventConsumer struct {
	commandHandler       *handlers.CommandHandler
	categoryCacheService cacheservice.CategoryCacheService
}

func NewCategoryEventConsumer(commandHandler *handlers.CommandHandler, categoryCacheService cacheservice.CategoryCacheService) kafka.Consumer {
	return &categoryEventConsumer{
		commandHandler:       commandHandler,
		categoryCacheService: categoryCacheService,
	}
}

func (consumer *categoryEventConsumer) Consume(ctx context.Context, msg *kafka.ConsumerMessage) error {
	var event model.CategoryEvent
	if err := custom_json.Unmarshal(msg.Value, &event); err != nil {
		return err
	}
	err := consumer.commandHandler.IndexCategory.Handle(ctx, &commands.IndexCategory{Id: event.Id})
	if err != nil {
		return err
	}
	return consumer.categoryCacheService.InvalidateById(ctx, event.Id)
}
