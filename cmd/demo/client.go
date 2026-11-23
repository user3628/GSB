package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type demoClient struct {
	baseURL string
	http    *http.Client
}

type apiEnvelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *apiErrorBody   `json:"error"`
	Meta  struct {
		ElapsedMS  int64 `json:"elapsed_ms"`
		ServedAtMS int64 `json:"served_at_ms"`
	} `json:"meta"`
}

type apiErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
}

type apiFailure struct {
	HTTPStatus int
	Code       string
	Message    string
	Context    map[string]any
}

func (e *apiFailure) Error() string {
	return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.HTTPStatus)
}

func newDemoClient(baseURL string) *demoClient {
	return &demoClient{baseURL: baseURL, http: &http.Client{Timeout: 12 * time.Second}}
}

func (c *demoClient) do(method, path string, body, target any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	request, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return err
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("decode %s %s: %w (%s)", method, path, err, string(raw))
	}
	if !envelope.OK || response.StatusCode >= 400 {
		code := "E_INTERNAL"
		message := string(raw)
		context := map[string]any{}
		if envelope.Error != nil {
			code = envelope.Error.Code
			message = envelope.Error.Message
			context = envelope.Error.Context
		}
		return &apiFailure{HTTPStatus: response.StatusCode, Code: code, Message: message, Context: context}
	}
	if target != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, target); err != nil {
			return fmt.Errorf("decode data for %s: %w", path, err)
		}
	}
	return nil
}

func (c *demoClient) waitReady() error {
	deadline := time.Now().Add(10 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		request, _ := http.NewRequest(http.MethodGet, c.baseURL+"/api/health", nil)
		response, err := c.http.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Errorf("health status %d", response.StatusCode)
		} else {
			last = err
		}
		time.Sleep(120 * time.Millisecond)
	}
	return fmt.Errorf("workbench health timeout: %v", last)
}
