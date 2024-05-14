package cacheservice

import (
	"context"
	"fmt"
	"github.com/patrickmn/go-cache"
	"presentation-advert-consumer/application/cacheservice"
	"presentation-advert-consumer/infrastructure/repository"
	"presentation-advert-consumer/model/model_cache"
	"time"
)

type categoryCacheService struct {
	inMemCache         *cache.Cache
	categoryRepository *repository.CategoryElasticRepository
}

func NewCategoryCacheService(
	categoryRepository *repository.CategoryElasticRepository,
) cacheservice.CategoryCacheService {
	return &categoryCacheService{
		inMemCache:         cache.New(72*time.Hour, 1*time.Hour),
		categoryRepository: categoryRepository,
	}
}

func (service *categoryCacheService) GetById(ctx context.Context, id int64) (*model_cache.Category, error) {
	data, found := service.inMemCache.Get(fmt.Sprint(id))
	if !found {
		category, err := service.categoryRepository.GetById(ctx, id)
		if err != nil {
			return nil, err
		}
		categoryName := &model_cache.Category{
			Id:               category.Id,
			Name:             category.Name,
			Version:          category.Version,
			CreatedBy:        category.CreatedBy,
			CreationDate:     category.CreationDate,
			ModifiedBy:       category.ModifiedBy,
			LastModifiedDate: category.LastModifiedDate,
			CacheUpdatedDate: time.Now(),
		}
		service.inMemCache.Set(fmt.Sprint(id), categoryName, 0)
		return categoryName, nil
	}
	return data.(*model_cache.Category), nil
}

func (service *categoryCacheService) InvalidateById(ctx context.Context, id int64) error {
	service.inMemCache.Delete(fmt.Sprint(id))
	_, err := service.GetById(ctx, id)
	return err
}
