package advert_api

import (
	"context"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"presentation-advert-consumer/application/client"
	"presentation-advert-consumer/infrastructure/configuration/client_config"
	"presentation-advert-consumer/infrastructure/configuration/client_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_error"
	"presentation-advert-consumer/model/model_client"
)

type advertApiClient struct {
	client_config.BaseClient
}

func NewAdvertApiClient(configMap client_config.ConfigMap, agentName string) (client.AdvertApiClient, error) {
	config, err := configMap.GetConfig("advertApiClient")
	if err != nil {
		return nil, err
	}
	return &advertApiClient{
		client_config.BaseClient{
			Name:      "advertApiClient",
			AgentName: agentName,
			Config:    config,
			Client: &fasthttp.Client{
				MaxConnsPerHost:     config.MaxConnections,
				MaxIdleConnDuration: config.MaxIdleConDuration,
				ReadTimeout:         config.ReadTimeout,
				WriteTimeout:        config.WriteTimeout,
			},
		},
	}, nil
}

func (client *advertApiClient) GetAdvertById(ctx context.Context, id int64) (*model_client.AdvertResponse, error) {
	var response model_client.AdvertResponse
	if err := client.GetRequest(ctx, fmt.Sprintf("/adverts/%d", id), &response); err != nil {
		var statusCodeError client_error.HttpStatusCodeError
		isStatusCodeError := errors.As(err, &statusCodeError)
		if isStatusCodeError && statusCodeError.StatusCode() == 404 {
			return nil, custom_error.NotFoundErrWithArgs("Advert not found by id: %s", id)
		}
		return nil, err
	}
	return &response, nil
}

func (client *advertApiClient) GetCategoryById(ctx context.Context, id int64) (*model_client.CategoryResponse, error) {
	var response model_client.CategoryResponse
	if err := client.GetRequest(ctx, fmt.Sprintf("/categories/%d", id), &response); err != nil {
		var statusCodeError client_error.HttpStatusCodeError
		isStatusCodeError := errors.As(err, &statusCodeError)
		if isStatusCodeError && statusCodeError.StatusCode() == 404 {
			return nil, custom_error.NotFoundErrWithArgs("Category not found by id: %s", id)
		}
		return nil, err
	}
	return &response, nil
}
