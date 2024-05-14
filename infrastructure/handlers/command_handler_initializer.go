package handlers

import (
	"presentation-advert-consumer/application/cacheservice"
	"presentation-advert-consumer/application/client"
	"presentation-advert-consumer/application/handlers"
	"presentation-advert-consumer/application/handlers/command_handlers"
	"presentation-advert-consumer/application/tracers"
	"presentation-advert-consumer/infrastructure/repository"
	infraTracers "presentation-advert-consumer/infrastructure/tracers"
)

func InitializeCommandHandler(
	advertApiClient client.AdvertApiClient,
	categoryRepository *repository.CategoryElasticRepository,
	advertRepository *repository.AdvertElasticRepository,
	categoryCacheService cacheservice.CategoryCacheService,
) (*handlers.CommandHandler, error) {
	tracer := []tracers.Tracer{
		infraTracers.NewOtelTracer(),
	}
	commandHandler := &handlers.CommandHandler{}
	commandHandler.IndexCategory = handlers.NewCommandHandlerDecorator(command_handlers.NewIndexCategoryCommandHandler(
		advertApiClient,
		categoryRepository,
	), tracer)
	commandHandler.IndexAdvert = handlers.NewCommandHandlerDecorator(command_handlers.NewIndexAdvertCommandHandler(
		advertApiClient,
		advertRepository,
		categoryCacheService,
	), tracer)
	return commandHandler, nil
}
