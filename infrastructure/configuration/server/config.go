package server

import (
	"github.com/labstack/echo/v4"
	echoSwagger "github.com/swaggo/echo-swagger"
	"net/http"
)

func RegisterSwaggerRedirect(e *echo.Echo) {
	e.GET("/swagger/*", echoSwagger.WrapHandler)
	e.GET("/", swaggerRedirect)
}

func RegisterHealthCheck(e *echo.Echo) {
	e.GET("/healthcheck", health)
}

func swaggerRedirect(c echo.Context) error {
	return c.Redirect(http.StatusSeeOther, "/swagger/index.html")
}

func health(c echo.Context) error {
	return c.NoContent(http.StatusOK)
}
