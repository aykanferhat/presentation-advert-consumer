package repository

import (
	"context"
	"fmt"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/infrastructure/configuration/elastic/elasticv7"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"presentation-advert-consumer/model/model_repository"
)

type CategoryElasticRepository struct {
	elastic.BaseGenericRepository[string, model_repository.Category]
}

func NewCategoryElasticRepository(elasticClientMap elasticv7.ClusterClientMap, clusterName string, indexName string) (*CategoryElasticRepository, error) {
	if client, exists := elasticClientMap[clusterName]; exists {
		return &CategoryElasticRepository{
			BaseGenericRepository: elasticv7.NewBaseGenericRepository(client, indexName, mapToEventForCategory, mapToIdForCategory),
		}, nil
	}
	return nil, custom_error.NewConfigNotFoundErr("elastic client not found")
}

func (repository *CategoryElasticRepository) Save(ctx context.Context, model *model_repository.Category) error {
	id := fmt.Sprint(model.Id)
	err := repository.IndexDocument(ctx, &elastic.IndexDocument{Id: id, Routing: id, Body: model})
	if err != nil {
		log.Errorf("An error occurred when indexing category, id: %d, err: %s", model.Id, err.Error())
		return err
	}
	log.Infof("Indexed category, id: %d", model.Id)
	return nil
}

func (repository *CategoryElasticRepository) GetById(ctx context.Context, id int64) (*model_repository.Category, error) {
	return repository.BaseGenericRepository.GetById(ctx, fmt.Sprint(id), "")
}

func mapToIdForCategory(searchHit *elastic.SearchHit) (string, error) {
	return searchHit.Id, nil
}

func mapToEventForCategory(searchHit *elastic.SearchHit) (string, *model_repository.Category, error) {
	id, err := mapToIdForCategory(searchHit)
	if err != nil {
		return "", nil, err
	}
	var event model_repository.Category
	if err := custom_json.Unmarshal(searchHit.Source, &event); err != nil {
		return "", nil, err
	}
	return id, &event, nil
}
