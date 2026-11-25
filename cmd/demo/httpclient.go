package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type apiError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context"`
	Status  int            `json:"-"`
	Body    map[string]any `json:"-"`
}

func (e *apiError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

type httpClient struct {
	base string
	http *http.Client
}

func newHTTPClient(base string) *httpClient {
	return &httpClient{base: base, http: &http.Client{Timeout: 15 * time.Second}}
}

func (c *httpClient) post(path string, body any) (map[string]any, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, fmt.Errorf("status %d non-json %s: %w", resp.StatusCode, string(data), err)
	}
	if resp.StatusCode >= 400 {
		apiErr := &apiError{Status: resp.StatusCode, Body: decoded, Context: map[string]any{}}
		apiErr.Code, _ = decoded["code"].(string)
		apiErr.Message, _ = decoded["message"].(string)
		if ctx, ok := decoded["context"].(map[string]any); ok {
			apiErr.Context = ctx
		}
		if nested, ok := decoded["error"].(map[string]any); ok {
			if ctx, ok := nested["context"].(map[string]any); ok {
				apiErr.Context = ctx
			}
		}
		return decoded, apiErr
	}
	return decoded, nil
}

func (c *httpClient) get(path string) (map[string]any, error) {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return decoded, fmt.Errorf("GET %s status %d: %s", path, resp.StatusCode, string(data))
	}
	return decoded, nil
}

func mustAPICode(err error, code string) (*apiError, error) {
	var apiErr *apiError
	if !asAPIError(err, &apiErr) {
		return nil, fmt.Errorf("expected API error %s, got %v", code, err)
	}
	if apiErr.Code != code {
		return apiErr, fmt.Errorf("expected %s, got %s (%s)", code, apiErr.Code, apiErr.Message)
	}
	return apiErr, nil
}

func asAPIError(err error, target **apiError) bool {
	for err != nil {
		if e, ok := err.(*apiError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func intField(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

func stringField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
