package command_handlers

import (
	"context"
	"presentation-advert-consumer/application/client"
	"presentation-advert-consumer/application/commands"
	"presentation-advert-consumer/application/handlers"
	"presentation-advert-consumer/application/repository"
	"presentation-advert-consumer/model/model_repository"
	"time"
)

type indexCategoryCommandHandler struct {
	advertApiClient    client.AdvertApiClient
	categoryRepository repository.CategoryRepository
}

func NewIndexCategoryCommandHandler(
	advertApiClient client.AdvertApiClient,
	categoryRepository repository.CategoryRepository,
) handlers.CommandHandlerInterface[*commands.IndexCategory, error] {
	return &indexCategoryCommandHandler{
		advertApiClient:    advertApiClient,
		categoryRepository: categoryRepository,
	}
}

func (handler *indexCategoryCommandHandler) Handle(ctx context.Context, command *commands.IndexCategory) error {
	categoryResponse, err := handler.advertApiClient.GetCategoryById(ctx, command.Id)
	if err != nil {
		return err
	}
	category := &model_repository.Category{
		Id:        command.Id,
		Name:      categoryResponse.Name,
		Version:   categoryResponse.Version,
		IndexedAt: time.Now(),
	}
	return handler.categoryRepository.Save(ctx, category)
}
