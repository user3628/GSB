package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"recoveryengine/internal/model"
)

type Kind string

const (
	KindProbe   Kind = "probe"
	KindRecover Kind = "recover"
)

type Operation struct {
	Kind           Kind
	ComponentID    string
	URL            string
	Method         string
	Timeout        time.Duration
	Body           string
	ExpectedStatus int
}

type Result struct {
	OK               bool   `json:"ok"`
	Code             string `json:"code"`
	Message          string `json:"message"`
	HTTPStatus       int    `json:"http_status,omitempty"`
	TimeoutMs        int64  `json:"timeout_ms"`
	DurationMs       int64  `json:"duration_ms"`
	Target           string `json:"target"`
	Method           string `json:"method"`
	ConnectionClosed bool   `json:"connection_closed"`
}

type Client struct{}

func (c *Client) Execute(ctx context.Context, operation Operation) Result {
	started := time.Now()
	result := Result{
		Code:      "OK",
		Target:    operation.URL,
		Method:    operation.Method,
		TimeoutMs: operation.Timeout.Milliseconds(),
	}
	if operation.Timeout <= 0 {
		result.Code = codeFor(operation.Kind, "timeout")
		result.Message = "operation timeout must be positive"
		return result
	}
	if operation.Method == "" {
		if operation.Kind == KindRecover {
			operation.Method = http.MethodPost
		} else {
			operation.Method = http.MethodGet
		}
	}
	result.Method = operation.Method
	if !model.IsLoopbackURL(operation.URL) {
		result.Code = model.CodeSpecInvalid
		result.Message = "only loopback HTTP targets are permitted"
		return result
	}

	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: operation.Timeout, KeepAlive: -1}).DialContext,
		DisableKeepAlives:     true,
		DisableCompression:    true,
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   0,
		IdleConnTimeout:       time.Second,
		TLSHandshakeTimeout:   operation.Timeout,
		ResponseHeaderTimeout: operation.Timeout,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     false,
	}
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()

	requestCtx, cancel := context.WithTimeout(ctx, operation.Timeout)
	defer cancel()

	var body io.Reader
	if operation.Body != "" {
		body = strings.NewReader(operation.Body)
	}
	request, err := http.NewRequestWithContext(requestCtx, operation.Method, operation.URL, body)
	if err != nil {
		result.Code = model.CodeSpecInvalid
		result.Message = err.Error()
		result.DurationMs = time.Since(started).Milliseconds()
		return result
	}
	request.Header.Set("User-Agent", "recovery-coordinator/1.0")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := client.Do(request)
	result.DurationMs = time.Since(started).Milliseconds()
	if err != nil {
		result.ConnectionClosed = true
		if isTimeout(err, requestCtx) {
			result.Code = codeFor(operation.Kind, "timeout")
			result.Message = "request deadline exceeded; transport connection was closed"
			return result
		}
		if isConnectionRefused(err) {
			result.Code = codeFor(operation.Kind, "refused")
			result.Message = "connection refused by local target"
			return result
		}
		result.Code = codeFor(operation.Kind, "network")
		result.Message = err.Error()
		return result
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	_, readErr := io.CopyN(io.Discard, response.Body, 4096)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		result.DurationMs = time.Since(started).Milliseconds()
		if isTimeout(readErr, requestCtx) {
			result.Code = codeFor(operation.Kind, "timeout")
			result.Message = "response body deadline exceeded; transport connection was closed"
			result.ConnectionClosed = true
			return result
		}
		result.Code = codeFor(operation.Kind, "network")
		result.Message = readErr.Error()
		result.ConnectionClosed = true
		return result
	}
	result.ConnectionClosed = true
	if operation.ExpectedStatus > 0 {
		if response.StatusCode != operation.ExpectedStatus {
			result.Code = codeFor(operation.Kind, "rejected")
			result.Message = fmt.Sprintf("target returned HTTP %d, expected %d", response.StatusCode, operation.ExpectedStatus)
			return result
		}
	} else if response.StatusCode < 200 || response.StatusCode >= 300 {
		result.Code = codeFor(operation.Kind, "rejected")
		result.Message = fmt.Sprintf("target returned HTTP %d", response.StatusCode)
		return result
	}
	result.OK = true
	return result
}

func isTimeout(err error, ctx context.Context) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return true
	}
	return false
}

func isConnectionRefused(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var opError *net.OpError
	if errors.As(err, &opError) && errors.Is(opError.Err, syscall.ECONNREFUSED) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func codeFor(kind Kind, category string) string {
	if kind == KindRecover {
		switch category {
		case "timeout":
			return model.CodeRecoverTimeout
		case "refused":
			return model.CodeRecoverConnectionRef
		case "rejected":
			return model.CodeRecoverRejected
		default:
			return model.CodeRecoverNetwork
		}
	}
	switch category {
	case "timeout":
		return model.CodeProbeTimeout
	case "refused":
		return model.CodeProbeConnectionRefused
	case "rejected":
		return model.CodeProbeRejected
	default:
		return model.CodeProbeNetwork
	}
}
