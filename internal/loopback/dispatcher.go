package loopback

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"leasearbiter/internal/core"
)

type Dispatcher struct {
	baseURL string
	timeout time.Duration
}

type DispatchOptions struct {
	Inject  string
	DelayMS int64
}

func NewDispatcher(baseURL string, timeout time.Duration) *Dispatcher {
	return &Dispatcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		timeout: timeout,
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, decision core.HealDecision, options DispatchOptions) (core.HealReceipt, *core.BizError) {
	return d.dispatch(ctx, "/v1/heal", decision, options)
}

func (d *Dispatcher) DispatchRaw(ctx context.Context, decision core.HealDecision, options DispatchOptions, requestID string, sentAtMS int64) (core.HealReceipt, *core.BizError) {
	if requestID == "" {
		requestID = newRequestID()
	}
	request := decision.Request(requestID, sentAtMS)
	return d.post(ctx, "/v1/heal", request, options)
}

func (d *Dispatcher) dispatch(ctx context.Context, path string, decision core.HealDecision, options DispatchOptions) (core.HealReceipt, *core.BizError) {
	request := decision.Request(newRequestID(), time.Now().UnixMilli())
	return d.post(ctx, path, request, options)
}

func (d *Dispatcher) post(ctx context.Context, path string, request core.HealRequest, options DispatchOptions) (core.HealReceipt, *core.BizError) {
	body, err := json.Marshal(request)
	if err != nil {
		return core.HealReceipt{}, core.Biz(core.CodeHealFailed, err.Error())
	}
	// Each dispatch owns its transport and connection tracker. Concurrent
	// timeouts therefore close only their own sockets and cannot perturb peers.
	tracker := &connTracker{connections: make(map[*trackedConn]struct{})}
	transport := &http.Transport{
		Proxy:             nil,
		DialContext:       tracker.dialContext,
		DisableKeepAlives: true,
		MaxIdleConns:      0,
	}
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()
	defer tracker.closeAll()
	requestCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, d.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return core.HealReceipt{}, core.Biz(core.CodeHealFailed, err.Error())
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if options.Inject != "" {
		httpRequest.Header.Set(HeaderInject, options.Inject)
	}
	if options.DelayMS > 0 {
		httpRequest.Header.Set(HeaderDelayMS, fmt.Sprintf("%d", options.DelayMS))
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		// A timeout is a distinct business outcome. Ensure no partially open
		// connection survives the failed dispatch.
		tracker.closeAll()
		if requestCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return core.HealReceipt{}, core.Biz(core.CodeHealTimeout, fmt.Sprintf("heal dispatch exceeded %s and its connection was closed", d.timeout))
		}
		if errors.Is(err, context.Canceled) {
			return core.HealReceipt{}, core.Biz(core.CodeHealFailed, "heal dispatch canceled")
		}
		return core.HealReceipt{}, core.Biz(core.CodeHealRejected, "heal endpoint rejected or could not be reached: "+err.Error())
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if readErr != nil {
		return core.HealReceipt{}, core.Biz(core.CodeHealFailed, "read heal response: "+readErr.Error())
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return core.HealReceipt{RequestID: request.RequestID, HolderID: request.HolderID}, &core.BizError{
			Code:    core.CodeHealRejected,
			Message: fmt.Sprintf("heal endpoint returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload))),
		}
	}
	var receipt core.HealReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		return core.HealReceipt{}, core.Biz(core.CodeHealFailed, "invalid heal receipt: "+err.Error())
	}
	return receipt, nil
}

func newRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("heal-%d", time.Now().UnixNano())
	}
	return "heal-" + hex.EncodeToString(value[:])
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type connTracker struct {
	mu          sync.Mutex
	connections map[*trackedConn]struct{}
}

func (t *connTracker) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tracked := &trackedConn{Conn: conn, tracker: t}
	t.mu.Lock()
	t.connections[tracked] = struct{}{}
	t.mu.Unlock()
	return tracked, nil
}

func (t *connTracker) closeAll() {
	t.mu.Lock()
	connections := make([]*trackedConn, 0, len(t.connections))
	for conn := range t.connections {
		connections = append(connections, conn)
	}
	t.connections = make(map[*trackedConn]struct{})
	t.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Conn.Close()
	}
}

type trackedConn struct {
	net.Conn
	tracker *connTracker
}

func (c *trackedConn) Close() error {
	c.tracker.mu.Lock()
	delete(c.tracker.connections, c)
	c.tracker.mu.Unlock()
	return c.Conn.Close()
}
