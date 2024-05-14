package elasticv7

import (
	"context"
	"github.com/avast/retry-go"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/infrastructure/configuration/log"
	"sync"
	"time"
)

type baseGenericRepository[ID comparable, T any] struct {
	*baseRepository
	mapFunc   func(searchHit *elastic.SearchHit) (ID, *T, error)
	mapIdFunc func(searchHit *elastic.SearchHit) (ID, error)
}

func NewBaseGenericRepository[ID comparable, T any](
	client *elasticsearch.Client,
	IndexName string,
	mapFunc func(searchHit *elastic.SearchHit) (ID, *T, error),
	mapIdFunc func(searchHit *elastic.SearchHit) (ID, error),
) elastic.BaseGenericRepository[ID, T] {
	return &baseGenericRepository[ID, T]{
		mapFunc:        mapFunc,
		mapIdFunc:      mapIdFunc,
		baseRepository: newBaseRepository(client, IndexName),
	}
}

func (repository *baseGenericRepository[ID, T]) GetById(ctx context.Context, documentId string, routingId string) (*T, error) {
	var document elastic.SearchHit
	err := retry.Do(
		func() error {
			req := esapi.GetRequest{
				Index:      repository.IndexName,
				DocumentID: documentId,
				Routing:    routingId,
			}
			response, err := req.Do(ctx, repository.Client)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.IsError() {
				if response.StatusCode == 404 {
					return custom_error.NotFoundErrWithArgs("GetById, Document not found by id %s", documentId)
				}
				return custom_error.InternalServerErrWithArgs("GetById, %s Index returned an error with status code: %d", repository.IndexName, response.StatusCode)
			}
			if err := custom_json.Decode(response.Body, &document); err != nil {
				return err
			}
			if !document.Found {
				return custom_error.NotFoundErrWithArgs("GetById, Document not found by id %s", documentId)
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(isRetryable),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	_, result, err := repository.mapFunc(&document)
	return result, err
}

func (repository *baseGenericRepository[ID, T]) GetSearchHits(ctx context.Context, query map[string]interface{}) (map[ID]*T, error) {
	searchResponse, err := repository.Search(ctx, query)
	if err != nil {
		return nil, err
	}
	searchHitMap := make(map[ID]*T)
	for _, searchHit := range searchResponse.Hits.Hits {
		id, mappedHit, err := repository.mapFunc(searchHit)
		if err != nil {
			return nil, err
		}
		searchHitMap[id] = mappedHit
	}
	return searchHitMap, err
}

func (repository *baseGenericRepository[ID, T]) GetIdsChannel(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (<-chan []ID, <-chan error) {
	idsChan := make(chan []ID)
	errChan := make(chan error, 1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		searchResponse, err := repository.scrollSearch(ctx, query, scrollSize, scrollDuration)
		if err != nil {
			log.Errorf("GetIdsChannel, Error while get response for %s query: %s, err: %s", repository.IndexName, query, err.Error())
			errChan <- err
			return
		}
		scrollId := searchResponse.ScrollId
		defer func() {
			_, _ = repository.Client.ClearScroll(repository.Client.ClearScroll.WithScrollID(scrollId))
		}()
		for {
			ids, err := repository.mapToIds(searchResponse)
			if err != nil {
				errChan <- err
				return
			}
			idsChan <- ids
			if len(ids) < scrollSize {
				return
			}
			searchResponse, err = repository.scrolling(ctx, scrollId, scrollDuration)
			if err != nil {
				log.Errorf("GetIdsChannel, Error while scrolling for %s query: %s, err: %s", repository.IndexName, query, err.Error())
				errChan <- err
				return
			}
			scrollId = searchResponse.ScrollId
		}
	}(&waitGroup)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(idsChan)
		close(errChan)
	}(&waitGroup)
	return idsChan, errChan
}

func (repository *baseGenericRepository[ID, T]) GetSearchHitsChannel(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (<-chan map[ID]*T, <-chan error) {
	searchHitMapChan := make(chan map[ID]*T)
	errChan := make(chan error, 1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		searchResponse, err := repository.scrollSearch(ctx, query, scrollSize, scrollDuration)
		if err != nil {
			errChan <- err
			return
		}
		scrollId := searchResponse.ScrollId
		defer func() {
			_, _ = repository.Client.ClearScroll(repository.Client.ClearScroll.WithScrollID(scrollId))
		}()
		for {
			searchHitMap, err := repository.mapResponse(searchResponse)
			if err != nil {
				errChan <- err
				return
			}
			searchHitMapChan <- searchHitMap
			if len(searchResponse.Hits.Hits) < scrollSize {
				return
			}
			searchResponse, err = repository.scrolling(ctx, scrollId, scrollDuration)
			if err != nil {
				errChan <- err
				return
			}
			scrollId = searchResponse.ScrollId
		}
	}(&waitGroup)
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(searchHitMapChan)
		close(errChan)
	}(&waitGroup)
	return searchHitMapChan, errChan
}

func (repository *baseGenericRepository[ID, T]) GetSearchHitsUsingScroll(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) (map[ID]*T, error) {
	searchHitsChan, errChan := repository.GetSearchHitsChannel(ctx, query, scrollSize, scrollDuration)
	allSearchHits := make(map[ID]*T, 0)
	for {
		select {
		case searchHitsMap, ok := <-searchHitsChan:
			if !ok {
				searchHitsChan = nil
				break
			}
			if len(searchHitsMap) == 0 {
				continue
			}
			for id, searchHit := range searchHitsMap {
				allSearchHits[id] = searchHit
			}
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				break
			}
			return nil, err
		}
		if searchHitsChan == nil || errChan == nil {
			break
		}
	}
	return allSearchHits, nil
}

func (repository *baseGenericRepository[ID, T]) GetIds(ctx context.Context, query map[string]interface{}) ([]ID, error) {
	searchResponse, err := repository.Search(ctx, query)
	if err != nil {
		return nil, err
	}
	return repository.mapToIds(searchResponse)
}

func (repository *baseGenericRepository[ID, T]) GetIdsUsingScroll(ctx context.Context, query map[string]interface{}, scrollSize int, scrollDuration time.Duration) ([]ID, error) {
	searchHitsChan, errChan := repository.GetIdsChannel(ctx, query, scrollSize, scrollDuration)
	allSearchHits := make([]ID, 0)
	for {
		select {
		case searchHitsMap, ok := <-searchHitsChan:
			if !ok {
				searchHitsChan = nil
				break
			}
			if len(searchHitsMap) == 0 {
				continue
			}
			for id, searchHit := range searchHitsMap {
				allSearchHits[id] = searchHit
			}
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				break
			}
			return nil, err
		}
		if searchHitsChan == nil || errChan == nil {
			break
		}
	}
	return allSearchHits, nil
}

func (repository *baseGenericRepository[ID, T]) mapToIds(response *elastic.SearchResponse) ([]ID, error) {
	ids := make([]ID, 0, len(response.Hits.Hits))
	for _, searchHit := range response.Hits.Hits {
		id, err := repository.mapIdFunc(searchHit)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (repository *baseGenericRepository[ID, T]) mapResponse(searchResponse *elastic.SearchResponse) (map[ID]*T, error) {
	searchHitMap := make(map[ID]*T)
	for _, hit := range searchResponse.Hits.Hits {
		id, mappedHit, err := repository.mapFunc(hit)
		if err != nil {
			return nil, err
		}
		searchHitMap[id] = mappedHit
	}
	return searchHitMap, nil
}
