package repository

import (
	"context"
	"fmt"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/infrastructure/configuration/elastic/elasticv7"
	"presentation-advert-consumer/model/model_repository"
)

type AdvertRepository struct {
	elastic.BaseGenericRepository[string, model_repository.Advert]
}

func NewAdvertRepository(elasticClientMap elasticv7.ClusterClientMap, clusterName string, indexName string) (*AdvertRepository, error) {
	if client, exists := elasticClientMap[clusterName]; exists {
		return &AdvertRepository{
			BaseGenericRepository: elasticv7.NewBaseGenericRepository(client, indexName, mapToEventForAdvert, mapToIdForAdvert),
		}, nil
	}
	return nil, custom_error.NewConfigNotFoundErr("elastic client not found")
}

func (repository *AdvertRepository) Save(ctx context.Context, model *model_repository.Advert) error {
	id := fmt.Sprint(model.Id)
	return repository.IndexDocument(ctx, &elastic.IndexDocument{Id: id, Routing: id, Body: model})
}

func (repository *AdvertRepository) GetById(ctx context.Context, id int64) (*model_repository.Advert, error) {
	return repository.BaseGenericRepository.GetById(ctx, fmt.Sprint(id), "")
}

func mapToIdForAdvert(searchHit *elastic.SearchHit) (string, error) {
	return searchHit.Id, nil
}

func mapToEventForAdvert(searchHit *elastic.SearchHit) (string, *model_repository.Advert, error) {
	id, err := mapToIdForCategory(searchHit)
	if err != nil {
		return "", nil, err
	}
	var event model_repository.Advert
	if err := custom_json.Unmarshal(searchHit.Source, &event); err != nil {
		return "", nil, err
	}
	return id, &event, nil
}
