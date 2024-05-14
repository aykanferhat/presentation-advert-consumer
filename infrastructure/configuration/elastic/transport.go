package elastic

import (
	"bytes"
	"compress/gzip"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"strings"
)

type transport struct {
	client *fasthttp.Client
}

func NewTransport(elasticConfig *Config) *transport {
	client := &fasthttp.Client{
		MaxConnsPerHost:        fasthttp.DefaultMaxConnsPerHost,
		MaxIdleConnDuration:    fasthttp.DefaultMaxIdleConnDuration,
		DisablePathNormalizing: true,
	}
	client.MaxConnsPerHost = elasticConfig.MaxIdleConnPerHost
	client.MaxIdleConnDuration = elasticConfig.MaxIdleConnDuration
	if elasticConfig.ReadTimeout != 0 {
		client.ReadTimeout = elasticConfig.ReadTimeout
	}
	if elasticConfig.WriteTimeout != 0 {
		client.WriteTimeout = elasticConfig.WriteTimeout
	}
	return &transport{client: client}
}

// RoundTrip performs the request and returns a response or error
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	freq := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(freq)

	fastHttpResponse := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(fastHttpResponse)

	t.copyRequest(freq, req)

	err := t.client.Do(freq, fastHttpResponse)
	if err != nil {
		return nil, err
	}

	return t.toHttpResponse(fastHttpResponse)
}

// copyRequest converts a http.Request to fasthttp.Request
func (t *transport) copyRequest(dst *fasthttp.Request, src *http.Request) *fasthttp.Request {
	if src.Method == http.MethodGet && src.Body != nil {
		src.Method = http.MethodPost
	}
	dst.SetHost(src.Host)
	dst.SetRequestURI(src.URL.String())
	dst.Header.SetRequestURI(src.URL.String())
	dst.Header.SetMethod(src.Method)
	dst.Header.Set("Accept-Encoding", "gzip")

	for k, vv := range src.Header {
		for _, v := range vv {
			dst.Header.Set(k, v)
		}
	}

	if src.Body != nil {
		dst.SetBodyStream(src.Body, -1)
	}

	return dst
}

// toHttpResponse converts fasthttp.Response to http.Response
func (t *transport) toHttpResponse(src *fasthttp.Response) (*http.Response, error) {
	response := &http.Response{Header: make(http.Header)}
	response.StatusCode = src.StatusCode()

	src.Header.VisitAll(func(k, v []byte) {
		response.Header.Set(string(k), string(v))
	})

	responseStr := string(src.Body())

	contentEncoding := response.Header.Get("Content-Encoding")

	if contentEncoding == "gzip" {
		compressedBuffer := bytes.NewReader(src.Body())

		gzipReader, err := gzip.NewReader(compressedBuffer)

		if err != nil {
			return nil, err
		}

		uncompressedData := new(bytes.Buffer)
		_, err = uncompressedData.ReadFrom(gzipReader)

		if err != nil {
			return nil, err
		}

		responseStr = uncompressedData.String()
	}

	// Cast to a string to make a copy seeing as src.Body() won't
	// be valid after the response is released back to the pool (fasthttp.ReleaseResponse).
	response.Body = io.NopCloser(strings.NewReader(responseStr))

	return response, nil
}
