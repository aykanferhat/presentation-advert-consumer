package client

import (
	"context"
	"presentation-advert-consumer/model/model_client"
)

type AdvertApiClient interface {
	GetAdvertById(ctx context.Context, id int64) (*model_client.AdvertResponse, error)
	GetCategoryById(ctx context.Context, id int64) (*model_client.CategoryResponse, error)
}
