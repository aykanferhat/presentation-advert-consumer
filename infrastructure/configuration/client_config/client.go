package client_config

import (
	"context"
	"errors"
	"fmt"
	"github.com/avast/retry-go"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
	"presentation-advert-consumer/infrastructure/configuration/client_error"
	"presentation-advert-consumer/infrastructure/configuration/custom_json"
	"strings"
)

type BaseClient struct {
	Client    *fasthttp.Client
	Name      string
	AgentName string
	Config    *Config
}

func (client *BaseClient) GetRequest(ctx context.Context, path string, responseBody interface{}) error {
	return client.sendRequest(ctx, client.createSimpleRequest(ctx, path, fasthttp.MethodGet, nil), responseBody)
}

func (client *BaseClient) PostRequest(ctx context.Context, path string, requestBody, responseBody interface{}) error {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodPost, nil)
	body, err := jsoniter.Marshal(requestBody)
	if err != nil {
		return err
	}
	request.SetBody(body)
	return client.sendRequest(ctx, request, responseBody)
}

func (client *BaseClient) PostRequestWithoutResponse(ctx context.Context, path string, requestBody interface{}) error {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodPost, nil)
	body, err := jsoniter.Marshal(requestBody)
	if err != nil {
		return err
	}
	request.SetBody(body)
	return client.sendRequestWithoutResponse(ctx, request)
}

func (client *BaseClient) PutRequest(ctx context.Context, path string, requestBody, responseBody interface{}) error {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodPut, nil)
	body, err := jsoniter.Marshal(requestBody)
	if err != nil {
		return err
	}
	request.SetBody(body)
	return client.sendRequest(ctx, request, responseBody)
}

func (client *BaseClient) PostRequestAndGetScrollId(ctx context.Context, path string, requestBody, responseBody interface{}) (string, error) {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodPost, nil)
	body, err := jsoniter.Marshal(requestBody)
	if err != nil {
		return "", err
	}
	request.SetBody(body)
	return client.sendRequestAndGetHeaderScrollId(request, responseBody)
}

func (client *BaseClient) GetRequestAndGetScrollId(ctx context.Context, path string, responseBody interface{}) (string, error) {
	return client.sendRequestAndGetHeaderScrollId(client.createSimpleRequest(ctx, path, fasthttp.MethodGet, nil), responseBody)
}

func (client *BaseClient) GetRequestWithHeaders(ctx context.Context, path string, responseBody interface{}, headers map[string]string) (string, error) {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodGet, headers)
	uri := request.URI().String()
	return uri, client.sendRequest(ctx, request, responseBody)
}

func (client *BaseClient) PostRequestWithHeaders(ctx context.Context, path string, requestBody, responseBody interface{}, headers map[string]string) (string, error) {
	request := client.createSimpleRequest(ctx, path, fasthttp.MethodPost, headers)
	uri := request.URI().String()
	body, err := jsoniter.Marshal(requestBody)
	if err != nil {
		return uri, err
	}
	request.SetBody(body)
	return uri, client.sendRequest(ctx, request, responseBody)
}

func (client *BaseClient) createSimpleRequest(ctx context.Context, path, method string, headers map[string]string) *fasthttp.Request {
	request := fasthttp.AcquireRequest()
	request.Header.SetMethod(method)
	request.Header.SetContentType("application/json")
	request.Header.Add(fasthttp.HeaderAcceptEncoding, "br, gzip")
	request.Header.SetUserAgent(client.AgentName)
	var requestURI string
	if strings.HasPrefix(path, "/") {
		requestURI = fmt.Sprintf("%s%s", client.Config.Url, path)
	} else {
		requestURI = fmt.Sprintf("%s/%s", client.Config.Url, path)
	}
	request.SetRequestURI(requestURI)
	if len(headers) != 0 {
		for k, v := range headers {
			request.Header.Add(k, v)
		}
	}
	return request
}

func (client *BaseClient) sendRequest(ctx context.Context, request *fasthttp.Request, responseBody interface{}) error {
	defer fasthttp.ReleaseRequest(request)
	return retry.Do(
		func() error {
			response := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(response)
			if err := client.Client.Do(request, response); err != nil {
				return err
			}
			if response.StatusCode() >= 400 {
				return client_error.NewHttpClientErrorFastHttp(response, client.Name)
			}
			body, err := client_error.ReadBodyFastHttp(response)
			if err != nil {
				return err
			}
			if err = custom_json.Unmarshal(body, responseBody); err != nil {
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(func(err error) bool {
			var codeError client_error.HttpStatusCodeError
			if errors.As(err, &codeError) {
				return !IsNotFoundErr(err) && codeError.StatusCode() != 400
			}
			return !IsNotFoundErr(err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
}

func (client *BaseClient) sendRequestWithoutResponse(ctx context.Context, request *fasthttp.Request) error {
	defer fasthttp.ReleaseRequest(request)
	return retry.Do(
		func() error {
			response := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(response)
			if err := client.Client.Do(request, response); err != nil {
				return err
			}
			if response.StatusCode() >= 400 {
				return client_error.NewHttpClientErrorFastHttp(response, client.Name)
			}
			_, err := client_error.ReadBodyFastHttp(response)
			if err != nil {
				return err
			}
			return nil
		},
		retry.Context(ctx),
		retry.RetryIf(func(err error) bool {
			var codeError client_error.HttpStatusCodeError
			if errors.As(err, &codeError) {
				return !IsNotFoundErr(err) && codeError.StatusCode() != 400
			}
			return !IsNotFoundErr(err)
		}),
		retry.Attempts(5),
		retry.LastErrorOnly(true),
	)
}

func (client *BaseClient) sendRequestAndGetHeaderScrollId(request *fasthttp.Request, responseBody interface{}) (string, error) {
	defer fasthttp.ReleaseRequest(request)
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(response)
	if err := client.Client.Do(request, response); err != nil {
		return "", err
	}
	if response.StatusCode() >= 400 {
		return "", client_error.NewHttpClientErrorFastHttp(response, client.Name)
	}
	body, err := client_error.ReadBodyFastHttp(response)
	if err != nil {
		return "", err
	}
	if err = jsoniter.Unmarshal(body, responseBody); err != nil {
		return "", err
	}
	scrollId := response.Header.Peek("x-scrollid")
	return string(scrollId), nil
}

func IsNotFoundErr(err error) bool {
	var statusCodeError client_error.HttpStatusCodeError
	isStatusCodeError := errors.As(err, &statusCodeError)
	return isStatusCodeError && statusCodeError.StatusCode() == 404
}
