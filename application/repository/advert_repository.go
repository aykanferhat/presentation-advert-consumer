package repository

import (
	"context"
	"presentation-advert-consumer/model/model_repository"
)

type AdvertRepository interface {
	Save(ctx context.Context, model *model_repository.Advert) error
	GetById(ctx context.Context, id int64) (*model_repository.Advert, error)
}
