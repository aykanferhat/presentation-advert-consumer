package cacheservice

import (
	"context"
	"presentation-advert-consumer/model/model_cache"
)

type CategoryCacheService interface {
	GetById(ctx context.Context, id int64) (*model_cache.Category, error)
	InvalidateById(ctx context.Context, id int64) error
}
