package writeback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"driftengine/internal/errs"
	"driftengine/internal/spec"
)

type FieldsResponse struct {
	ID            string                       `json:"id"`
	Namespace     string                       `json:"namespace,omitempty"`
	Fields        spec.Fields                  `json:"fields"`
	SourceMarkers map[string]spec.SourceMarker `json:"source_markers,omitempty"`
	ObservedAt    time.Time                    `json:"observed_at,omitempty"`
}

type writeRequest struct {
	Fields spec.Fields `json:"fields"`
}

type Client struct {
	BaseURL   string
	Timeout   time.Duration
	transport *http.Transport
	http      *http.Client
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	transport.DisableKeepAlives = true
	transport.MaxIdleConns = 0
	transport.IdleConnTimeout = time.Second
	transport.TLSHandshakeTimeout = timeout
	c := &Client{
		BaseURL:   strings.TrimRight(baseURL, "/"),
		Timeout:   timeout,
		transport: transport,
		http:      &http.Client{Transport: transport, Timeout: timeout},
	}
	return c
}

func (c *Client) WriteFields(parent context.Context, objectID string, fields spec.Fields) error {
	body, err := json.Marshal(writeRequest{Fields: fields})
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	url := c.BaseURL + "/loopback/v1/objects/" + objectID + "/fields"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return errs.New(errs.Internal, err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		if isDeadline(ctx, err) {
			c.transport.CloseIdleConnections()
			return errs.New(errs.WriteTimeout, "write request exceeded independent timeout", map[string]string{"object_id": objectID})
		}
		if isRefused(err) {
			return errs.New(errs.EndpointRefused, "loopback endpoint refused connection", err.Error())
		}
		return errs.New(errs.Internal, "write request failed", err.Error())
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errs.New(errs.Internal, "write endpoint returned non-success status", resp.Status)
	}
	return nil
}

func (c *Client) ReadState(parent context.Context, objectID string) (FieldsResponse, error) {
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	url := c.BaseURL + "/loopback/v1/objects/" + objectID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return FieldsResponse{}, errs.New(errs.Internal, err.Error())
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if isDeadline(ctx, err) {
			c.transport.CloseIdleConnections()
			return FieldsResponse{}, errs.New(errs.WriteTimeout, "readback request exceeded timeout", objectID)
		}
		if isRefused(err) {
			return FieldsResponse{}, errs.New(errs.EndpointRefused, "loopback endpoint refused connection", err.Error())
		}
		return FieldsResponse{}, errs.New(errs.Internal, "readback request failed", err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return FieldsResponse{ID: objectID, Fields: spec.Fields{}}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return FieldsResponse{}, errs.New(errs.Internal, "readback endpoint returned non-success status", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return FieldsResponse{}, errs.New(errs.Internal, err.Error())
	}
	var result FieldsResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&result); err != nil {
		return FieldsResponse{}, errs.New(errs.Internal, "readback response cannot be decoded", err.Error())
	}
	if result.Fields == nil {
		result.Fields = spec.Fields{}
	}
	return result, nil
}

func isDeadline(ctx context.Context, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isRefused(err error) bool {
	return strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "connect: connection refused")
}

func (c *Client) Close() {
	c.transport.CloseIdleConnections()
}

var _ = fmt.Sprintf
