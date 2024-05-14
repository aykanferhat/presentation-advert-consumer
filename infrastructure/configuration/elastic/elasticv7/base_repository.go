package elasticv7

import (
	"bytes"
	"context"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"time"
)

type baseRepository struct {
	Client      *elasticsearch.Client
	IndexName   string
	bulkIndexer *bulkIndexer
}

func NewBaseRepository(
	client *elasticsearch.Client,
	indexName string,
) elastic.BaseRepository {
	return &baseRepository{
		Client:      client,
		IndexName:   indexName,
		bulkIndexer: newBulkIndexer(client, indexName),
	}
}

func newBaseRepository(
	client *elasticsearch.Client,
	indexName string,
) *baseRepository {
	return &baseRepository{
		Client:      client,
		IndexName:   indexName,
		bulkIndexer: newBulkIndexer(client, indexName),
	}
}

func (repository *baseRepository) GetCount(ctx context.Context, query map[string]interface{}) (*elastic.CountResponse, error) {
	var countResponse elastic.CountResponse
	err := retry.Do(
		func() error {
			response, err := repository.Client.Count(
				repository.Client.Count.WithContext(ctx),
				repository.Client.Count.WithIndex(repository.IndexName),
				repository.Client.Count.WithBody(esutil.NewJSONReader(&query)),
			)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.IsError() {
				if response.StatusCode == 404 {
					return custom_error.NotFoundErrWithArgs("GetCount, Index returned 404")
				}
				return custom_error.InternalServerErrWithArgs("GetCount, %s Index returned an error with status code: %d, err: %s", repository.IndexName, response.StatusCode, response.String())
			}

			if err := custom_json.Decode(response.Body, &countResponse); err != nil {
				return err
			}
			if countResponse.Shards != nil && countResponse.Shards.Failed > 0 {
				shardInfoDetailsAsJson, err := custom_json.Marshal(countResponse.Shards)
				if err != nil {
					return custom_error.InternalServerErrWithArgs("GetCount, %d shard failure occurred during search query", countResponse.Shards.Failed)
				}
				return custom_error.InternalServerErrWithArgs("GetCount, %d shard failure occurred during search query: %s", countResponse.Shards.Failed, shardInfoDetailsAsJson)
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.Attempts(3),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return &countResponse, nil
}

func (repository *baseRepository) GetCustomSearchHitResultByQuery(ctx context.Context, query map[string]interface{}, result interface{}) error {
	searchResponse, err := repository.Search(ctx, query)
	if err != nil {
		return err
	}
	for _, searchHit := range searchResponse.Hits.Hits {
		if err := custom_json.Unmarshal(searchHit.Source, result); err != nil {
			return err
		}
	}
	return nil
}

func (repository *baseRepository) ExistsById(ctx context.Context, document *elastic.ExistsDocument) (bool, error) {
	var exists bool
	err := retry.Do(
		func() error {
			req := esapi.ExistsRequest{
				Index:      repository.IndexName,
				DocumentID: document.Id,
				Routing:    document.Routing,
			}
			response, err := req.Do(ctx, repository.Client)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.IsError() {
				if response.StatusCode == 404 {
					exists = false
					return nil
				}
				return custom_error.InternalServerErrWithArgs("ExistsById, %s Index returned an error with status code: %d", repository.IndexName, response.StatusCode)
			}
			exists = true
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (repository *baseRepository) IndexDocument(ctx context.Context, document *elastic.IndexDocument) error {
	reqBodyBytes := new(bytes.Buffer)
	if err := custom_json.Encode(reqBodyBytes, document.Body); err != nil {
		log.Errorf("IndexDocument, Json deserialization error, id: %s, err: %s", document.Id, err.Error())
		return err
	}
	return retry.Do(
		func() error {
			req := esapi.IndexRequest{
				Index:      repository.IndexName,
				DocumentID: document.Id,
				Routing:    document.Routing,
				Body:       reqBodyBytes,
				Refresh:    "false",
			}
			res, err := req.Do(ctx, repository.Client)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			if res.IsError() {
				if res.StatusCode == 404 {
					return custom_error.NotFoundErrWithArgs("IndexDocument, %s index not found", repository.IndexName)
				}
				return custom_error.InternalServerErrWithArgs("IndexDocument, %s index returned an error with status code: %d", repository.IndexName, res.StatusCode)
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.OnRetry(func(retryCount uint, err error) {
			log.Errorf("Error while get response for IndexDocument id: %s, err: %s", document.Id, err.Error())
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
}

func (repository *baseRepository) IndexDocuments(ctx context.Context, documents []*elastic.IndexDocument) error {
	if len(documents) == 0 {
		return nil
	}
	docs := make([]*elastic.BulkIndexerItem, 0, len(documents))
	for _, document := range documents {
		docs = append(docs, elastic.NewIndexAction(document.Id, document.Body, document.Routing))
	}
	return repository.bulkIndexer.ProcessItems(docs)
}

func (repository *baseRepository) DeleteDocuments(ctx context.Context, documents []*elastic.DeleteDocument) error {
	if len(documents) == 0 {
		return nil
	}
	docs := make([]*elastic.BulkIndexerItem, 0, len(documents))
	for _, document := range documents {
		docs = append(docs, elastic.NewDeleteAction(document.Id, document.Routing))
	}
	return repository.bulkIndexer.ProcessItems(docs)
}

func (repository *baseRepository) DeleteById(ctx context.Context, document *elastic.DeleteDocument) error {
	return retry.Do(
		func() error {
			req := esapi.DeleteRequest{
				Index:      repository.IndexName,
				DocumentID: document.Id,
				Routing:    document.Routing,
				Timeout:    2 * time.Second,
			}
			response, err := req.Do(ctx, repository.Client)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.IsError() {
				if response.StatusCode == 404 {
					return nil
				}
				return custom_error.InternalServerErrWithArgs("RemoveById, %s Index returned an error with status code: %d", repository.IndexName, response.StatusCode)
			}
			return err
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
}

func (repository *baseRepository) Search(ctx context.Context, query map[string]interface{}) (*elastic.SearchResponse, error) {
	var response *esapi.Response
	err := retry.Do(
		func() error {
			var err error
			response, err = repository.Client.Search(
				repository.Client.Search.WithContext(ctx),
				repository.Client.Search.WithIndex(repository.IndexName),
				repository.Client.Search.WithBody(esutil.NewJSONReader(&query)),
				repository.Client.Search.WithTrackTotalHits(false),
			)
			if err != nil {
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.OnRetry(func(retryCount uint, err error) {
			log.Errorf("Search, %s error, %v", repository.IndexName, err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return repository.parseElasticsearchResponse(response)
}

func (repository *baseRepository) SearchWithSize(ctx context.Context, query map[string]interface{}, size int) (*elastic.SearchResponse, error) {
	var response *esapi.Response
	err := retry.Do(
		func() error {
			var err error
			response, err = repository.Client.Search(
				repository.Client.Search.WithContext(ctx),
				repository.Client.Search.WithIndex(repository.IndexName),
				repository.Client.Search.WithBody(esutil.NewJSONReader(&query)),
				repository.Client.Search.WithSize(size),
			)
			if err != nil {
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.OnRetry(func(retryCount uint, err error) {
			log.Errorf("Search, %s error, %v", repository.IndexName, err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return repository.parseElasticsearchResponse(response)
}

func (repository *baseRepository) scrollSearch(ctx context.Context, query map[string]interface{}, size int, duration time.Duration) (*elastic.SearchResponse, error) {
	var response *esapi.Response
	err := retry.Do(
		func() error {
			var err error
			response, err = repository.Client.Search(
				repository.Client.Search.WithContext(ctx),
				repository.Client.Search.WithIndex(repository.IndexName),
				repository.Client.Search.WithBody(esutil.NewJSONReader(&query)),
				repository.Client.Search.WithSize(size),
				repository.Client.Search.WithScroll(duration),
			)
			if err != nil {
				log.Errorf("ScrollSearch, Error while get response for %s query: %s, err: %s", repository.IndexName, query, err.Error())
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.OnRetry(func(retryCount uint, err error) {
			log.Errorf("ScrollSearch, %s error, %v", repository.IndexName, err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return repository.parseElasticsearchResponse(response)
}

func (repository *baseRepository) scrolling(ctx context.Context, scrollId string, scrollDuration time.Duration) (*elastic.SearchResponse, error) {
	var response *esapi.Response
	err := retry.Do(
		func() error {
			var err error
			response, err = repository.Client.Scroll(
				repository.Client.Scroll.WithScrollID(scrollId),
				repository.Client.Scroll.WithScroll(scrollDuration),
				repository.Client.Scroll.WithContext(context.Background()),
			)
			if err != nil {
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(func(err error) bool {
			return true
		}),
		retry.OnRetry(func(retryCount uint, err error) {
			log.Errorf("Scrolling, Error while get response for %s query: %s", repository.IndexName, err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return repository.parseElasticsearchResponse(response)
}

func (repository *baseRepository) parseElasticsearchResponse(res *esapi.Response) (*elastic.SearchResponse, error) {
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err := custom_json.Decode(res.Body, &e); err != nil {
			return nil, err
		} else {
			return nil, custom_error.InternalServerErrWithArgs("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		}
	}
	var response elastic.SearchResponse
	if err := custom_json.Decode(res.Body, &response); err != nil {
		return nil, err
	}
	if response.Shards != nil && response.Shards.Failed > 0 {
		searchResultAsJson, err := custom_json.Marshal(response)
		if err != nil {
			return nil, err
		}
		errorMessage := fmt.Sprintf("parseElasticsearchResponse, %d shard failure occurred while making initial search query: %v", response.Shards.Failed, string(searchResultAsJson))
		log.Errorf("%s", errorMessage)
		return nil, custom_error.InternalServerErr(errorMessage)
	}
	return &response, nil
}

func isRetryable(err error) bool {
	return !custom_error.IsNotFoundError(err)
}
