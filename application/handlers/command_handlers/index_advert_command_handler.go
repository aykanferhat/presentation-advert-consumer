package command_handlers

import (
	"context"
	"presentation-advert-consumer/application/cacheservice"
	"presentation-advert-consumer/application/client"
	"presentation-advert-consumer/application/commands"
	"presentation-advert-consumer/application/handlers"
	"presentation-advert-consumer/application/repository"
	"presentation-advert-consumer/model/model_repository"
)

type indexAdvertCommandHandler struct {
	advertApiClient      client.AdvertApiClient
	advertRepository     repository.AdvertRepository
	categoryCacheService cacheservice.CategoryCacheService
}

func NewIndexAdvertCommandHandler(
	advertApiClient client.AdvertApiClient,
	advertRepository repository.AdvertRepository,
	categoryCacheService cacheservice.CategoryCacheService,
) handlers.CommandHandlerInterface[*commands.IndexAdvert, error] {
	return &indexAdvertCommandHandler{
		advertApiClient:      advertApiClient,
		advertRepository:     advertRepository,
		categoryCacheService: categoryCacheService,
	}
}

func (handler *indexAdvertCommandHandler) Handle(ctx context.Context, command *commands.IndexAdvert) error {
	advertResponse, err := handler.advertApiClient.GetAdvertById(ctx, command.Id)
	if err != nil {
		return err
	}
	categoryResponse, err := handler.categoryCacheService.GetById(ctx, advertResponse.CategoryId)
	if err != nil {
		return err
	}
	advert := &model_repository.Advert{
		Id:               advertResponse.Id,
		Title:            advertResponse.Title,
		Description:      advertResponse.Description,
		Version:          advertResponse.Version,
		CreatedBy:        advertResponse.CreatedBy,
		CreationDate:     advertResponse.CreationDate,
		ModifiedBy:       advertResponse.ModifiedBy,
		LastModifiedDate: advertResponse.LastModifiedDate,
		Category: model_repository.AdvertCategory{
			Id:               categoryResponse.Id,
			Name:             categoryResponse.Name,
			Version:          categoryResponse.Version,
			CreatedBy:        categoryResponse.CreatedBy,
			CreationDate:     categoryResponse.CreationDate,
			ModifiedBy:       categoryResponse.ModifiedBy,
			LastModifiedDate: categoryResponse.LastModifiedDate},
	}
	return handler.advertRepository.Save(ctx, advert)
}
