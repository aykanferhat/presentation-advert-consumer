package client_error

import (
	"compress/gzip"
	"fmt"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"presentation-advert-consumer/infrastructure/configuration/log"
)

func NewHttpClientErrorFastHttp(response *fasthttp.Response, clientName string) HttpStatusCodeError {
	body, err := ReadBodyFastHttp(response)
	if err != nil {
		log.Errorf("[%s] - An client_error occurred while extracting body of fast http response as bytes. Err: %s", clientName, err.Error())
	}
	header := make(http.Header)
	response.Header.VisitAll(func(key, value []byte) {
		header.Add(string(key), string(value))
	})
	return newHttpClientError(
		response.StatusCode(),
		body,
		header,
	)
}

func ReadBodyFastHttp(response *fasthttp.Response) ([]byte, error) {
	contentEncoding := string(response.Header.Peek("content-Encoding"))
	if contentEncoding == "gzip" {
		return response.BodyGunzip()
	}
	if contentEncoding == "deflate" {
		return response.BodyInflate()
	}
	return response.Body(), nil
}

type HttpStatusCodeError interface {
	StatusCode() int
	Body() []byte
	Header() http.Header
	Error() string
}

type httpStatusCodeError struct {
	statusCode int
	body       []byte
	header     http.Header
}

func (e httpStatusCodeError) Error() string {
	return fmt.Sprintf("StatusCode: %d, Body: %s, Header: %v", e.statusCode, string(e.body), e.header)
}

func (e httpStatusCodeError) StatusCode() int {
	return e.statusCode
}

func (e httpStatusCodeError) Body() []byte {
	return e.body
}

func (e httpStatusCodeError) Header() http.Header {
	return e.header
}

func newHttpClientError(statusCode int, body []byte, header http.Header) HttpStatusCodeError {
	return httpStatusCodeError{
		statusCode: statusCode,
		body:       body,
		header:     header,
	}
}
func NewHttpClientError(response *http.Response) HttpStatusCodeError {
	body, err := ReadBody(response)
	if err != nil {
		log.Error("An error occurred while extracting body of fast http response as bytes.")
	}

	header := response.Header.Clone()
	return newHttpClientError(
		response.StatusCode,
		body,
		header,
	)
}

func ReadBody(response *http.Response) ([]byte, error) {
	defer func() {
		_ = response.Body.Close()
	}()

	if response.Uncompressed {
		return io.ReadAll(response.Body)
	}

	contentEncoding := response.Header.Get("content-Encoding")
	switch contentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(response.Body)
		if err != nil {
			return nil, err
		}
		defer func() {
			_ = gzipReader.Close()
		}()
		return io.ReadAll(gzipReader)
	default:
		return io.ReadAll(response.Body)
	}
}
