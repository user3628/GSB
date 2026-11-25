package main

import (
	"elastic-scale/internal/app"
	"elastic-scale/internal/httpapi"
)

func newHTTPServer(a *app.App) *httpapi.Server { return httpapi.New(a) }
