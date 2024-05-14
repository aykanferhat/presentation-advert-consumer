package elastic

import (
	"encoding/json"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"presentation-advert-consumer/util"
)

type EsObject map[string]interface{}

type EsArray []interface{}

type IndexDocument struct {
	Id      string      `json:"id"`
	Routing string      `json:"routing"`
	Body    interface{} `json:"body"`
}

type Action string

const (
	IndexAction  Action = "Index"
	DeleteAction Action = "Delete"
)

type BulkIndexerItem struct {
	Id      []byte
	Routing string
	Type    Action
	Source  interface{}
}

func NewDeleteAction(id string, routing string) *BulkIndexerItem {
	return &BulkIndexerItem{
		Id:      util.ToByte(id),
		Routing: routing,
		Type:    DeleteAction,
	}
}

func NewIndexAction(id string, source interface{}, routing string) *BulkIndexerItem {
	return &BulkIndexerItem{
		Id:      util.ToByte(id),
		Routing: routing,
		Source:  source,
		Type:    IndexAction,
	}
}

type DeleteDocument struct {
	Id      string `json:"id"`
	Routing string `json:"routing"`
}

type ExistsDocument struct {
	Id      string `json:"id"`
	Routing string `json:"routing"`
}

type GetByDocIdResponse struct {
	Id      string          `json:"_id"`
	Routing string          `json:"_routing"`
	Source  json.RawMessage `json:"_source"`
	Found   bool            `json:"found"`
}

type CountResponse struct {
	Shards *ShardsInfo `json:"_shards,omitempty"`
	Count  int64       `json:"count"`
}

type SearchResponse struct {
	Hits         *SearchHits         `json:"hits,omitempty"`
	Shards       *ShardsInfo         `json:"_shards,omitempty"`
	Aggregations AggregateDictionary `json:"aggregations,omitempty"`
	ScrollId     string              `json:"_scroll_id,omitempty"`
	TookInMillis int64               `json:"took,omitempty"`
	TimedOut     bool                `json:"timed_out,omitempty"`
}

type ShardsInfo struct {
	Failures   []*ShardFailure `json:"failures,omitempty"`
	Failed     int             `json:"failed"`
	Skipped    int             `json:"skipped,omitempty"`
	Successful int             `json:"successful"`
	Total      int             `json:"total"`
}

type ShardFailure struct {
	Reason  map[string]interface{} `json:"reason,omitempty"`
	Index   string                 `json:"_index,omitempty"`
	Node    string                 `json:"_node,omitempty"`
	Status  string                 `json:"status,omitempty"`
	Shard   int                    `json:"_shard,omitempty"`
	Primary bool                   `json:"primary,omitempty"`
}

type SearchHit struct {
	Version *int            `json:"_version,omitempty"`
	Id      string          `json:"_id"`
	Routing string          `json:"_routing"`
	Source  json.RawMessage `json:"_source"`
	Score   float32         `json:"_score"`
	Found   bool            `json:"found"`
}

type SearchHits struct {
	Total    *Total       `json:"total,omitempty"`
	MaxScore *float64     `json:"max_score,omitempty"`
	Hits     []*SearchHit `json:"hits,omitempty"`
}

type Total struct {
	Relation string `json:"relation"`
	Value    int64  `json:"value"`
}

type ErrorDetails struct {
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
}

type AggregateDictionary map[string]json.RawMessage

type TermsAggregate struct {
	Buckets                 []TermsAggregateBucket `json:"buckets"`
	DocCountErrorUpperBound int64                  `json:"doc_count_error_upper_bound"`
	SumOtherDocCount        int64                  `json:"sum_other_doc_count"`
}

type TermsAggregateBucket struct {
	AggregateDictionary
	Key      interface{}
	DocCount int64
}

func (a *TermsAggregateBucket) UnmarshalJSON(data []byte) error {
	var aggregateDictionary map[string]json.RawMessage
	if err := custom_json.Unmarshal(data, &aggregateDictionary); err != nil {
		return err
	}
	a.AggregateDictionary = aggregateDictionary
	if key, found := aggregateDictionary["key"]; found && key != nil {
		if err := custom_json.Unmarshal(key, &a.Key); err != nil {
			return err
		}
	}
	if docCount, found := aggregateDictionary["doc_count"]; found && docCount != nil {
		if err := custom_json.Unmarshal(docCount, &a.DocCount); err != nil {
			return err
		}
	}
	return nil
}

type TopHitsAggregate struct {
	AggregateDictionary
	Hits *SearchResponse
}

func (a *TopHitsAggregate) UnmarshalJSON(data []byte) error {
	var aggregateDictionary map[string]json.RawMessage
	if err := json.Unmarshal(data, &aggregateDictionary); err != nil {
		return err
	}
	a.AggregateDictionary = aggregateDictionary
	a.Hits = new(SearchResponse)
	if hits, found := aggregateDictionary["hits"]; found && hits != nil {
		if err := custom_json.Unmarshal(hits, &a.Hits); err != nil {
			return err
		}
	}
	return nil
}

func (aggregateDictionary AggregateDictionary) Terms(key string) (*TermsAggregate, bool) {
	rawValue, found := aggregateDictionary[key]
	if found {
		termsAggregate := new(TermsAggregate)
		if rawValue == nil {
			return termsAggregate, true
		}
		err := custom_json.Unmarshal(rawValue, &termsAggregate)
		if err == nil {
			return termsAggregate, true
		}
	}
	return nil, false
}

func (aggregateDictionary AggregateDictionary) TopHits(key string) (*TopHitsAggregate, bool) {
	rawValue, found := aggregateDictionary[key]
	if found {
		termsAggregate := new(TopHitsAggregate)
		if rawValue == nil {
			return termsAggregate, true
		}
		err := custom_json.Unmarshal(rawValue, termsAggregate)
		if err == nil {
			return termsAggregate, true
		}
	}
	return nil, false
}

type ClusterHealthResponse struct {
	Indices                        map[string]*ClusterIndexHealth `json:"indices"`
	ClusterName                    string                         `json:"cluster_name"`
	Status                         string                         `json:"status"`
	ActiveShardsPercent            string                         `json:"active_shards_percent"`
	TaskMaxWaitTimeInQueue         string                         `json:"task_max_waiting_in_queue"`
	InitializingShards             int                            `json:"initializing_shards"`
	ActiveShards                   int                            `json:"active_shards"`
	RelocatingShards               int                            `json:"relocating_shards"`
	ActivePrimaryShards            int                            `json:"active_primary_shards"`
	UnassignedShards               int                            `json:"unassigned_shards"`
	DelayedUnassignedShards        int                            `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks           int                            `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch          int                            `json:"number_of_in_flight_fetch"`
	NumberOfDataNodes              int                            `json:"number_of_data_nodes"`
	TaskMaxWaitTimeInQueueInMillis int                            `json:"task_max_waiting_in_queue_millis"`
	NumberOfNodes                  int                            `json:"number_of_nodes"`
	ActiveShardsPercentAsNumber    float64                        `json:"active_shards_percent_as_number"`
	TimedOut                       bool                           `json:"timed_out"`
}

type ClusterIndexHealth struct {
	Shards              map[string]*ClusterShardHealth `json:"shards"`
	Status              string                         `json:"status"`
	NumberOfShards      int                            `json:"number_of_shards"`
	NumberOfReplicas    int                            `json:"number_of_replicas"`
	ActivePrimaryShards int                            `json:"active_primary_shards"`
	ActiveShards        int                            `json:"active_shards"`
	RelocatingShards    int                            `json:"relocating_shards"`
	InitializingShards  int                            `json:"initializing_shards"`
	UnassignedShards    int                            `json:"unassigned_shards"`
}

type ClusterShardHealth struct {
	Status             string `json:"status"`
	PrimaryActive      bool   `json:"primary_active"`
	ActiveShards       int    `json:"active_shards"`
	RelocatingShards   int    `json:"relocating_shards"`
	InitializingShards int    `json:"initializing_shards"`
	UnassignedShards   int    `json:"unassigned_shards"`
}

type RangeSearchProperties struct {
	StartPartition interface{}
	EndPartition   interface{}
}
