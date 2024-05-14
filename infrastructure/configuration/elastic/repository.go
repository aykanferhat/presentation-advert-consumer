package elastic

import (
	"context"
	"time"
)

type BaseRepository interface {
	GetCount(ctx context.Context, query map[string]interface{}) (*CountResponse, error)
	ExistsById(ctx context.Context, document *ExistsDocument) (bool, error)
	DeleteById(ctx context.Context, document *DeleteDocument) error
	IndexDocument(ctx context.Context, document *IndexDocument) error
	IndexDocuments(ctx context.Context, documents []*IndexDocument) error
	DeleteDocuments(ctx context.Context, documents []*DeleteDocument) error
	Search(ctx context.Context, query map[string]interface{}) (*SearchResponse, error)
	SearchWithSize(ctx context.Context, query map[string]interface{}, size int) (*SearchResponse, error)
}

type BaseGenericRepository[ID comparable, T any] interface {
	BaseRepository
	GetById(ctx context.Context, documentId string, routingId string) (*T, error)
	GetSearchHits(ctx context.Context, query map[string]interface{}) (map[ID]*T, error)
	GetSearchHitsChannel(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (<-chan map[ID]*T, <-chan error)
	GetSearchHitsUsingScroll(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (map[ID]*T, error)
	GetIds(ctx context.Context, query map[string]interface{}) ([]ID, error)
	GetIdsChannel(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (<-chan []ID, <-chan error)
	GetIdsUsingScroll(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) ([]ID, error)
}
