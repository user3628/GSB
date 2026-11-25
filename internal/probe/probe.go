package probe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"auditdrift/internal/model"
	"auditdrift/internal/normalize"
)

type Client struct {
	BaseURL string
	Timeout time.Duration
	HTTP    *http.Client
}

type ClientConfig struct {
	BaseURL     string
	Timeout     time.Duration
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}

func NewClient(baseURL string, timeout time.Duration) *Client {
	return NewClientWithConfig(ClientConfig{BaseURL: baseURL, Timeout: timeout})
}

func NewClientWithConfig(cfg ClientConfig) *Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 0}
	dialContext := cfg.DialContext
	if dialContext == nil {
		dialContext = dialer.DialContext
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          4,
		IdleConnTimeout:       time.Second,
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   timeout,
		ExpectContinueTimeout: 100 * time.Millisecond,
	}
	return &Client{BaseURL: strings.TrimRight(cfg.BaseURL, "/"), Timeout: timeout, HTTP: &http.Client{Transport: transport}}
}

type Response struct {
	Host        string          `json:"host"`
	BaselineID  string          `json:"baseline_id"`
	Actual      json.RawMessage `json:"actual"`
	SampledAtMS int64           `json:"sampled_at_ms"`
}

func (c *Client) Probe(ctx context.Context, host, baselineID string) (model.Observation, error) {
	probeCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	requestURL := fmt.Sprintf("%s/probe?host=%s&baseline_id=%s", c.BaseURL, url.QueryEscape(host), url.QueryEscape(baselineID))
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, requestURL, nil)
	if err != nil {
		return model.Observation{}, err
	}
	req.Close = true
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return model.Observation{}, ctx.Err()
		}
		if isTimeoutError(err, probeCtx) {
			c.HTTP.CloseIdleConnections()
			return model.Observation{}, model.NewCodedError(model.ErrProbeTimeout, "probe %s/%s exceeded independent timeout %s: %v", host, baselineID, c.Timeout, err)
		}
		if isRefusedError(err) {
			return model.Observation{}, model.NewCodedError(model.ErrProbeRefused, "probe %s/%s connection refused: %v", host, baselineID, err)
		}
		return model.Observation{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return model.Observation{}, model.NewCodedError("E_PROBE_HTTP_STATUS", "probe %s/%s returned HTTP %d", host, baselineID, resp.StatusCode)
	}
	var dto Response
	if err := json.NewDecoder(resp.Body).Decode(&dto); err != nil {
		return model.Observation{}, model.NewCodedError(model.ErrValueTypeMismatch, "probe %s/%s returned unparsable value: %v", host, baselineID, err)
	}
	value, err := normalize.ParseJSONValue(dto.Actual)
	if err != nil {
		return model.Observation{}, model.NewCodedError(model.ErrValueTypeMismatch, "probe %s/%s: %v", host, baselineID, err)
	}
	ts := dto.SampledAtMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return model.Observation{
		Host:        dto.Host,
		BaselineID:  dto.BaselineID,
		Actual:      model.CanonicalValue(value),
		SampledAtMS: ts,
		Source:      "http_probe",
	}, nil
}

func isTimeoutError(err error, ctx context.Context) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isRefusedError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "connect: connection refused")
}

type ConnTracker struct {
	mu     sync.Mutex
	opened int
	closed int
	active map[net.Conn]struct{}
}

func NewConnTracker() *ConnTracker {
	return &ConnTracker{active: make(map[net.Conn]struct{})}
}

func (t *ConnTracker) Wrap(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		tc := &trackedConn{Conn: conn, tracker: t}
		t.mu.Lock()
		t.opened++
		t.active[tc] = struct{}{}
		t.mu.Unlock()
		return tc, nil
	}
}

func (t *ConnTracker) noteClose(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.active[conn]; ok {
		delete(t.active, conn)
		t.closed++
	}
}

func (t *ConnTracker) Stats() (opened, closed, active int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.opened, t.closed, len(t.active)
}

type trackedConn struct {
	net.Conn
	tracker *ConnTracker
	once    sync.Once
}

func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.tracker.noteClose(c)
	})
	return err
}
