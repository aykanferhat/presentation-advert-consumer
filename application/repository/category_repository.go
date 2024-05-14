package repository

import (
	"context"
	"presentation-advert-consumer/model/model_repository"
)

type CategoryRepository interface {
	Save(ctx context.Context, model *model_repository.Category) error
	GetById(ctx context.Context, id int64) (*model_repository.Category, error)
}
