package elasticv7

import (
	"bytes"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/infrastructure/configuration/elastic"
	"presentation-advert-consumer/util"
	"strings"
)

type bulkIndexer struct {
	client   *elasticsearch.Client
	typeName []byte

	batchSizeLimit     int
	batchByteSizeLimit int
	indexName          string
}

func newBulkIndexer(
	client *elasticsearch.Client,
	indexName string,
) *bulkIndexer {
	return &bulkIndexer{
		client:             client,
		indexName:          indexName,
		batchSizeLimit:     1000,
		batchByteSizeLimit: 10485760, // 10 mb,
	}
}

func (bi *bulkIndexer) ProcessItems(items []*elastic.BulkIndexerItem) error {
	batch := make([]byte, 0)
	itemCount := 0
	for _, action := range items {
		bytes, err := getActionJSON(action.Id, action.Type, bi.indexName, action.Routing, action.Source, bi.typeName)
		if err != nil {
			return err
		}
		itemCount++
		if itemCount > bi.batchSizeLimit || len(batch) > bi.batchByteSizeLimit {
			if err := bi.bulkRequest(batch); err != nil {
				return err
			}
			batch = batch[:0]
			itemCount = 0
			continue
		}
		batch = append(batch, bytes...)
	}
	if len(batch) == 0 {
		return nil
	}
	if err := bi.bulkRequest(batch); err != nil {
		return err
	}
	return nil
}

var (
	indexPrefix   = util.ToByte(`{"index":{"_index":"`)
	deletePrefix  = util.ToByte(`{"delete":{"_index":"`)
	idPrefix      = util.ToByte(`","_id":"`)
	typePrefix    = util.ToByte(`","_type":"`)
	routingPrefix = util.ToByte(`","routing":"`)
	postFix       = util.ToByte(`"}}`)
)

func getActionJSON(docID []byte, action elastic.Action, indexName string, routing string, source interface{}, typeName []byte) ([]byte, error) {
	var meta []byte
	if action == elastic.IndexAction {
		meta = indexPrefix
	} else {
		meta = deletePrefix
	}
	meta = append(meta, util.ToByte(indexName)...)
	meta = append(meta, idPrefix...)
	meta = append(meta, docID...)
	if routing != "" {
		meta = append(meta, routingPrefix...)
		meta = append(meta, util.ToByte(routing)...)
	}
	if typeName != nil {
		meta = append(meta, typePrefix...)
		meta = append(meta, typeName...)
	}
	meta = append(meta, postFix...)
	if action == elastic.IndexAction {
		bytes, err := custom_json.Marshal(source)
		if err != nil {
			return nil, err
		}
		meta = append(meta, '\n')
		meta = append(meta, bytes...)
	}
	meta = append(meta, '\n')
	return meta, nil
}

func (bi *bulkIndexer) bulkRequest(batch []byte) error {
	reader := bytes.NewReader(batch)
	r, err := bi.client.Bulk(reader)
	if err != nil {
		return err
	}
	err = hasResponseError(r)
	if err != nil {
		return err
	}
	return nil
}

func hasResponseError(r *esapi.Response) error {
	if r == nil {
		return fmt.Errorf("esapi response is nil")
	}
	if r.IsError() {
		return fmt.Errorf("bulkIndexer request has error %v", r.String())
	}
	rb := new(bytes.Buffer)

	defer r.Body.Close()
	_, err := rb.ReadFrom(r.Body)
	if err != nil {
		return err
	}
	b := make(map[string]any)
	err = custom_json.Unmarshal(rb.Bytes(), &b)
	if err != nil {
		return err
	}
	hasError, ok := b["errors"].(bool)
	if !ok || !hasError {
		return nil
	}
	return joinErrors(b)
}

func joinErrors(body map[string]any) error {
	var sb strings.Builder
	sb.WriteString("bulkIndexer request has error. Errors will be listed below:\n")

	items, ok := body["items"].([]any)
	if !ok {
		return nil
	}

	for _, i := range items {
		item, ok := i.(map[string]any)
		if !ok {
			continue
		}

		for _, v := range item {
			iv, ok := v.(map[string]any)
			if !ok {
				continue
			}

			if iv["error"] != nil {
				sb.WriteString(fmt.Sprintf("%v\n", i))
			}
		}
	}
	return fmt.Errorf(sb.String())
}
