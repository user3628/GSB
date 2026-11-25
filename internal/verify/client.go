package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"edgeconfig/pkg/edgeconfig"
)

type apiClient struct {
	baseURL string
	http    *http.Client
}

func newAPIClient(baseURL string) *apiClient {
	return &apiClient{baseURL: baseURL, http: &http.Client{Timeout: 20 * time.Second}}
}

type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details"`
	} `json:"error"`
}

func (c *apiClient) post(ctx context.Context, path string, body any, out any) (int, *edgeconfig.APIError, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 400 {
		var envelope errorEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			return resp.StatusCode, nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(payload))
		}
		apiErr := edgeconfig.NewAPIError(resp.StatusCode, envelope.Error.Code, envelope.Error.Message, envelope.Error.Details)
		return resp.StatusCode, apiErr, nil
	}
	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return resp.StatusCode, nil, fmt.Errorf("decode response: %w: %s", err, string(payload))
		}
	}
	return resp.StatusCode, nil, nil
}

func (c *apiClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("GET %s returned %d: %s", path, resp.StatusCode, string(payload))
	}
	return json.Unmarshal(payload, out)
}
