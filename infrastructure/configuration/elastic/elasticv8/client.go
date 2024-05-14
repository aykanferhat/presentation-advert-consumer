package elasticv8

import (
	"github.com/elastic/go-elasticsearch/v8"
	"net/http"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	elastic2 "presentation-advert-consumer/infrastructure/configuration/elastic"
	"strings"
)

func Initialize(elasticConfigMap elastic2.ConfigMap) (ClusterClientMap, error) {
	elasticClientMap := make(map[string]*elasticsearch.Client)
	for clusterName, config := range elasticConfigMap {
		client, err := newElasticClient(config)
		if err != nil {
			return nil, err
		}
		elasticClientMap[clusterName] = client
	}
	return elasticClientMap, nil
}

func newElasticClient(elasticConfig *elastic2.Config) (*elasticsearch.Client, error) {
	addresses := strings.ReplaceAll(elasticConfig.Addresses, " ", "")
	splitAddresses := strings.Split(addresses, ",")
	var transport http.RoundTripper = elastic2.NewTransport(elasticConfig)
	config := elasticsearch.Config{
		Addresses:             splitAddresses,
		DiscoverNodesOnStart:  elasticConfig.DiscoverNodesOnStart,
		DiscoverNodesInterval: elasticConfig.DiscoverNodesInterval,
		Transport:             transport,
	}
	return elasticsearch.NewClient(config)
}

type ClusterClientMap map[string]*elasticsearch.Client

func (c ClusterClientMap) GetConfig(name string) (*elasticsearch.Client, error) {
	if config, exists := c[strings.ToLower(name)]; exists {
		return config, nil
	}
	return nil, custom_error.NewConfigNotFoundErr(name)
}
