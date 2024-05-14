package custom_error

import (
	"errors"
	"fmt"
	"github.com/labstack/echo/v4"
	"net/http"
	"strings"
	"time"
)

const (
	resourceNotFoundTitle    = "Not found"
	badRequestFoundTitle     = "Bad request"
	internalServerErrorTitle = "Internal Server Error"
)

func NewConfigNotFoundErr(configName string) error {
	return InternalServerErrWithArgs("%s config not found", configName)
}

func NewConfigNotFoundErrWithArgs(configName string, a ...any) error {
	return InternalServerErrWithArgs(configName, a...)
}

func CustomEchoHTTPErrorHandler(err error, c echo.Context) {
	var (
		customError = &CustomError{
			Detail: err.Error(),
		}
	)
	if ce, ok := err.(*CustomError); ok {
		customError = ce
	} else if he, ok := err.(*echo.HTTPError); ok {
		customError.Status = he.Code
	} else {
		customError.Status = http.StatusInternalServerError
		customError.Title = internalServerErrorTitle
		customError.Detail = err.Error()
	}
	customError.RequestMethod = c.Request().Method
	customError.RequestUri = c.Request().RequestURI
	customError.Instant = time.Now()

	if customError.Title == "" {
		if customError.Status == http.StatusBadRequest {
			customError.Title = badRequestFoundTitle
		} else if customError.Status == http.StatusInternalServerError {
			customError.Title = internalServerErrorTitle
		}
	}

	if !c.Response().Committed {
		if c.Request().Method == http.MethodHead { // Issue #608
			_ = c.NoContent(customError.Status)
		} else {
			_ = c.JSON(customError.Status, customError)
		}
	}
}

type CustomError struct {
	Title         string    `json:"title"`
	Status        int       `json:"status"`
	Detail        string    `json:"detail"`
	RequestUri    string    `json:"requestUri"`
	RequestMethod string    `json:"requestMethod"`
	Instant       time.Time `json:"instant"`
}

func (err CustomError) Error() string {
	return err.Detail
}

func BadRequestErr(detail string) error {
	return makeCustomErr(http.StatusBadRequest, detail, badRequestFoundTitle)
}

func BadRequestErrWithArgs(detail string, a ...any) error {
	return makeCustomErr(http.StatusBadRequest, fmt.Sprintf(detail, a...), badRequestFoundTitle)
}

func InternalServerErr(detail string) error {
	return makeCustomErr(http.StatusInternalServerError, detail, internalServerErrorTitle)
}

func InternalServerErrWithArgs(detail string, a ...any) error {
	return makeCustomErr(http.StatusInternalServerError, fmt.Sprintf(detail, a...), internalServerErrorTitle)
}

func NotFoundErr(detail string) error {
	return makeCustomErr(http.StatusNotFound, detail, resourceNotFoundTitle)
}

func NotFoundErrWithArgs(detail string, a ...any) error {
	return makeCustomErr(http.StatusNotFound, fmt.Sprintf(detail, a...), resourceNotFoundTitle)
}

func makeCustomErr(code int, detail string, title string) error {
	return &CustomError{
		Title:   title,
		Detail:  detail,
		Status:  code,
		Instant: time.Now(),
	}
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var ce *CustomError
	if errors.As(err, &ce) {
		if ce.Status == http.StatusNotFound {
			return true
		}
	}
	if strings.Contains(err.Error(), "not found") {
		return true
	}
	return false
}

func IsInternalServerErr(err error) bool {
	var ce *CustomError
	if errors.As(err, &ce) {
		if ce.Status == http.StatusInternalServerError {
			return true
		}
	}
	return false
}
