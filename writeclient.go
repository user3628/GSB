package convergence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ConnTracker struct {
	mu         sync.Mutex
	conns      map[*trackedConn]struct{}
	dialCount  atomic.Int64
	closeCount atomic.Int64
}

type connOwnerContextKey struct{}

type requestConnOwner struct {
	mu    sync.Mutex
	conns []*trackedConn
}

func (o *requestConnOwner) add(conn *trackedConn) {
	if o == nil || conn == nil {
		return
	}
	o.mu.Lock()
	o.conns = append(o.conns, conn)
	o.mu.Unlock()
}

func (o *requestConnOwner) closeAll() {
	if o == nil {
		return
	}
	o.mu.Lock()
	conns := append([]*trackedConn(nil), o.conns...)
	o.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

type trackedConn struct {
	net.Conn
	tracker *ConnTracker
	once    sync.Once
	closed  chan struct{}
}

func NewConnTracker() *ConnTracker {
	return &ConnTracker{conns: map[*trackedConn]struct{}{}}
}

func (t *ConnTracker) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tc := &trackedConn{Conn: conn, tracker: t, closed: make(chan struct{})}
	t.mu.Lock()
	t.conns[tc] = struct{}{}
	t.mu.Unlock()
	if owner, ok := ctx.Value(connOwnerContextKey{}).(*requestConnOwner); ok {
		owner.add(tc)
	}
	t.dialCount.Add(1)
	return tc, nil
}

func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		close(c.closed)
		if c.tracker != nil {
			c.tracker.closeCount.Add(1)
			c.tracker.mu.Lock()
			delete(c.tracker.conns, c)
			c.tracker.mu.Unlock()
		}
	})
	return err
}

func (t *ConnTracker) CloseAll() {
	t.mu.Lock()
	conns := make([]*trackedConn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (t *ConnTracker) DialCount() int64  { return t.dialCount.Load() }
func (t *ConnTracker) CloseCount() int64 { return t.closeCount.Load() }

func (t *ConnTracker) WaitClosed(expected int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if t.CloseCount() >= expected {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return t.CloseCount() >= expected
}

type WriteClient struct {
	Timeout time.Duration
	Tracker *ConnTracker
	client  *http.Client
}

func NewWriteClient(timeout time.Duration) *WriteClient {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	tracker := NewConnTracker()
	transport := &http.Transport{
		DialContext:           tracker.DialContext,
		DisableKeepAlives:     true,
		DisableCompression:    true,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       time.Second,
		TLSHandshakeTimeout:   time.Second,
		ExpectContinueTimeout: 100 * time.Millisecond,
	}
	return &WriteClient{
		Timeout: timeout,
		Tracker: tracker,
		client:  &http.Client{Transport: transport},
	}
}

type writeRequestBody struct {
	RequestID       string   `json:"request_id"`
	ExpectedFields  FieldSet `json:"expected_fields"`
	ResourceVersion string   `json:"resource_version,omitempty"`
}

type readbackResponseBody struct {
	ObjectID     string   `json:"object_id"`
	ClusterID    string   `json:"cluster_id"`
	ActualFields FieldSet `json:"actual_fields"`
	ResourceVer  string   `json:"resource_version,omitempty"`
}

type Readback struct {
	Fields          FieldSet
	ResourceVersion string
	HTTPStatus      int
}

// WriteAndReadback performs a real HTTP PUT, then GET and exact readback comparison.
func (c *WriteClient) WriteAndReadback(parent context.Context, endpoint string, object Object, requestID string, timeout time.Duration) (*Readback, error) {
	if c == nil {
		c = NewWriteClient(0)
	}
	if timeout <= 0 {
		timeout = c.Timeout
	}
	if timeout <= 0 {
		c.Timeout = 5 * time.Second
		timeout = c.Timeout
	}
	url, err := objectURL(endpoint, object)
	if err != nil {
		return nil, WrapError(CodeSpecInvalid, "invalid write endpoint", err, map[string]any{"object_id": object.ID, "endpoint": endpoint})
	}

	body, err := json.Marshal(writeRequestBody{RequestID: requestID, ExpectedFields: object.Expected, ResourceVersion: object.ResourceVer})
	if err != nil {
		return nil, WrapError(CodeSpecInvalid, "cannot encode write request", err, map[string]any{"object_id": object.ID})
	}

	putOwner := &requestConnOwner{}
	dialContext := context.WithValue(parent, connOwnerContextKey{}, putOwner)
	putCtx, putCancel := context.WithTimeout(dialContext, timeout)
	defer putOwner.closeAll()
	req, err := http.NewRequestWithContext(putCtx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		putOwner.closeAll()
		putCancel()
		return nil, WrapError(CodeWriteFailed, "cannot create write request", err, map[string]any{"object_id": object.ID})
	}
	req.Header.Set("Content-Type", "application/json")
	req.Close = true
	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(parent.Err(), context.DeadlineExceeded) || errors.Is(parent.Err(), context.Canceled) {
			putOwner.closeAll()
			putCancel()
			return nil, WrapError(CodeRoundBudget, "round context ended during write", parent.Err(), map[string]any{"object_id": object.ID})
		}
		if errors.Is(putCtx.Err(), context.DeadlineExceeded) || isTimeoutError(err) {
			putOwner.closeAll()
			if transport, ok := c.client.Transport.(*http.Transport); ok {
				transport.CloseIdleConnections()
			}
			putCancel()
			return nil, NewError(CodeWriteTimeout, "write timed out and underlying connection was closed", map[string]any{
				"object_id":    object.ID,
				"timeout":      timeout.String(),
				"connections":  c.Tracker.DialCount(),
				"closed_conns": c.Tracker.CloseCount(),
			})
		}
		putCancel()
		return nil, WrapError(CodeWriteFailed, "write request failed", err, map[string]any{"object_id": object.ID})
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	putCancel()
	putOwner.closeAll()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, NewError(CodeWriteFailed, "write endpoint returned non-2xx", map[string]any{"object_id": object.ID, "status_code": resp.StatusCode})
	}

	getOwner := &requestConnOwner{}
	getDialContext := context.WithValue(parent, connOwnerContextKey{}, getOwner)
	getCtx, getCancel := context.WithTimeout(getDialContext, timeout)
	defer getOwner.closeAll()
	defer getCancel()
	getReq, err := http.NewRequestWithContext(getCtx, http.MethodGet, url, nil)
	if err != nil {
		getOwner.closeAll()
		return nil, WrapError(CodeWriteFailed, "cannot create readback request", err, map[string]any{"object_id": object.ID})
	}
	getReq.Close = true
	getResp, err := c.client.Do(getReq)
	if err != nil {
		if errors.Is(getCtx.Err(), context.DeadlineExceeded) || isTimeoutError(err) {
			getOwner.closeAll()
			return nil, NewError(CodeWriteTimeout, "readback timed out", map[string]any{"object_id": object.ID, "timeout": timeout.String()})
		}
		return nil, WrapError(CodeWriteFailed, "readback request failed", err, map[string]any{"object_id": object.ID})
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		return nil, NewError(CodeWriteFailed, "readback endpoint returned non-200", map[string]any{"object_id": object.ID, "status_code": getResp.StatusCode})
	}
	data, err := io.ReadAll(io.LimitReader(getResp.Body, 16<<20))
	if err != nil {
		return nil, WrapError(CodeWriteFailed, "cannot read readback body", err, map[string]any{"object_id": object.ID})
	}
	var decoded readbackResponseBody
	if err := decodeUseNumber(data, &decoded); err != nil {
		return nil, WrapError(CodeWriteFailed, "cannot decode readback body", err, map[string]any{"object_id": object.ID})
	}
	normalized, err := normalizeFieldSet(decoded.ActualFields)
	if err != nil {
		return nil, err
	}
	expectedCanonical, _ := CanonicalJSON(object.Expected)
	actualCanonical, _ := CanonicalJSON(normalized)
	if !bytes.Equal(expectedCanonical, actualCanonical) {
		return nil, NewError(CodeApplyMismatch, "readback fields do not match write declaration", map[string]any{
			"object_id":       object.ID,
			"cluster_id":      object.ClusterID,
			"expected_fields": object.Expected,
			"actual_fields":   normalized,
		})
	}
	return &Readback{Fields: normalized, ResourceVersion: decoded.ResourceVer, HTTPStatus: resp.StatusCode}, nil
}

func objectURL(base string, object Object) (string, error) {
	if strings.TrimSpace(object.Endpoint) != "" && base == "" {
		base = object.Endpoint
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("endpoint must be absolute http URL: %q", base)
	}
	return strings.TrimRight(base, "/") +
		"/api/v1/clusters/" + url.PathEscape(object.ClusterID) +
		"/objects/" + url.PathEscape(object.ID), nil
}

func isTimeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
