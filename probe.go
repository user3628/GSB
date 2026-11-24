package ledger

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	ProbeOutcomeOK    = "E_PROBE_OK"
	ProbeModeOK       = "ok"
	ProbeModeRefused  = "refused"
	ProbeModeTimeout  = "timeout"
	ProbeModeMismatch = "mismatch"
	ProbeModeLate     = "late_response"
)

type ProbeInjection struct {
	Mode       string `json:"mode,omitempty"`
	RouteKey   string `json:"route_key,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	DelayMS    int64  `json:"delay_ms,omitempty"`
	WrongOwner string `json:"wrong_owner,omitempty"`
}

type ProbeRequest struct {
	BatchID         string          `json:"batch_id,omitempty"`
	RouteKey        string          `json:"route_key"`
	Endpoint        string          `json:"endpoint,omitempty"`
	BaseURL         string          `json:"base_url,omitempty"`
	ExpectedOwner   string          `json:"expected_owner_version,omitempty"`
	TimeoutMS       int64           `json:"timeout_ms,omitempty"`
	LateThresholdMS int64           `json:"late_threshold_ms,omitempty"`
	Injection       *ProbeInjection `json:"injection,omitempty"`
}

type ProbeResult struct {
	BatchID          string         `json:"batch_id"`
	RouteKey         string         `json:"route_key"`
	Endpoint         string         `json:"endpoint"`
	ExpectedOwner    string         `json:"expected_owner_version,omitempty"`
	ObservedOwner    string         `json:"observed_owner_version,omitempty"`
	Code             string         `json:"code"`
	OK               bool           `json:"ok"`
	HTTPStatus       int            `json:"http_status,omitempty"`
	ElapsedMS        int64          `json:"elapsed_ms"`
	TimedOut         bool           `json:"timed_out,omitempty"`
	ConnectionClosed bool           `json:"connection_closed,omitempty"`
	SlotReleased     bool           `json:"slot_released"`
	Detail           map[string]any `json:"detail,omitempty"`
}

type ProbeResponseBody struct {
	RouteKey     string `json:"route_key"`
	OwnerVersion string `json:"owner_version"`
	Status       string `json:"status"`
}

type trackedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		close(c.closed)
	})
	return err
}

type connectionRegistry struct {
	mu    sync.Mutex
	conns map[string][]*trackedConn
}

func newConnectionRegistry() *connectionRegistry {
	return &connectionRegistry{conns: map[string][]*trackedConn{}}
}

func (r *connectionRegistry) add(key string, conn *trackedConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[key] = append(r.conns[key], conn)
}

func (r *connectionRegistry) closeKey(key string) {
	r.mu.Lock()
	conns := append([]*trackedConn(nil), r.conns[key]...)
	r.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (r *connectionRegistry) waitClosed(key string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		conns := append([]*trackedConn(nil), r.conns[key]...)
		r.mu.Unlock()
		if len(conns) > 0 {
			allClosed := true
			for _, conn := range conns {
				select {
				case <-conn.closed:
				default:
					allClosed = false
				}
			}
			if allClosed {
				return true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func (w *Workbench) WaitProbeConnectionsClosed(batchID, routeKey string, timeout time.Duration) bool {
	return w.conns.waitClosed(probeConnKey(batchID, routeKey), timeout)
}

func probeConnKey(batchID, routeKey string) string { return batchID + "|" + routeKey }

func buildProbeEndpoint(request ProbeRequest) (string, *Error) {
	if request.Endpoint != "" {
		return request.Endpoint, nil
	}
	if request.Injection != nil && request.Injection.Mode == ProbeModeRefused && request.Injection.Endpoint != "" {
		return request.Injection.Endpoint, nil
	}
	if request.BaseURL == "" {
		return "", NewError(CodeProbeRefused, "probe endpoint or base_url is required").
			WithContext("route_key", request.RouteKey)
	}
	parsed, err := url.Parse(request.BaseURL)
	if err != nil {
		return "", NewError(CodeProbeRefused, "invalid probe base_url").WithContext("base_url", request.BaseURL)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/probe"
	query := parsed.Query()
	query.Set("route_key", request.RouteKey)
	if request.BatchID != "" {
		query.Set("batch_id", request.BatchID)
	}
	if request.ExpectedOwner != "" {
		query.Set("owner_version", request.ExpectedOwner)
	}
	if request.Injection != nil && (request.Injection.RouteKey == "" || request.Injection.RouteKey == request.RouteKey) {
		if request.Injection.Mode != "" {
			query.Set("mode", request.Injection.Mode)
		}
		if request.Injection.DelayMS > 0 {
			query.Set("delay_ms", intToString(request.Injection.DelayMS))
		}
		if request.Injection.WrongOwner != "" {
			query.Set("wrong_owner", request.Injection.WrongOwner)
		}
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func intToString(value int64) string {
	return fmtInt64(value)
}

// PerformProbe performs one real loopback HTTP probe. The concurrency slot is
// released before the result is returned, including timeout/error paths.
func (w *Workbench) PerformProbe(parent context.Context, request ProbeRequest) (result ProbeResult) {
	batchID := request.BatchID
	if batchID == "" {
		batchID = randomSuffix("batch-")
	}
	request.BatchID = batchID
	result.BatchID = batchID
	result.RouteKey = request.RouteKey
	result.ExpectedOwner = request.ExpectedOwner

	select {
	case w.probeSlots <- struct{}{}:
	case <-parent.Done():
		result.Code = CodeInternal
		result.Detail = map[string]any{"reason": "waiting_for_probe_slot_canceled"}
		return result
	}
	defer func() {
		<-w.probeSlots
		result.SlotReleased = true
	}()

	endpoint, endpointErr := buildProbeEndpoint(request)
	if endpointErr != nil {
		result.Endpoint = request.BaseURL
		result.Code = endpointErr.Code
		result.Detail = endpointErr.Context
		return result
	}
	result.Endpoint = endpoint

	timeout := time.Duration(request.TimeoutMS) * time.Millisecond
	if request.TimeoutMS <= 0 {
		timeout = w.cfg.ProbeTimeout
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	key := probeConnKey(batchID, request.RouteKey)
	tracker := &localConnTracker{}
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       time.Second,
		DisableKeepAlives:     true,
		ResponseHeaderTimeout: timeout,
		DialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(dialCtx, network, address)
			if err != nil {
				return nil, err
			}
			tracked := &trackedConn{Conn: conn, closed: make(chan struct{})}
			tracker.add(tracked)
			w.conns.add(key, tracked)
			return tracked, nil
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	started := time.Now()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		result.Code = CodeInternal
		result.Detail = map[string]any{"reason": err.Error()}
		result.ElapsedMS = time.Since(started).Milliseconds()
		return result
	}
	response, err := client.Do(httpRequest)
	elapsed := time.Since(started)
	result.ElapsedMS = elapsed.Milliseconds()
	if err != nil {
		if isTimeoutError(ctx, err) {
			cancel()
			tracker.closeAll()
			w.conns.closeKey(key)
			result.Code = CodeProbeTimeout
			result.TimedOut = true
			result.ConnectionClosed = true
			result.Detail = map[string]any{"timeout_ms": timeout.Milliseconds()}
			return result
		}
		if strings.Contains(strings.ToLower(err.Error()), "connection refused") {
			result.Code = CodeProbeRefused
			result.Detail = map[string]any{"transport_error": err.Error()}
			return result
		}
		result.Code = CodeProbeRefused
		result.Detail = map[string]any{"transport_error": err.Error(), "classified": "network_unreachable_as_refused"}
		return result
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	var parsed ProbeResponseBody
	if err := decodeJSON(body, &parsed); err != nil {
		result.Code = CodeProbeMismatch
		result.Detail = map[string]any{"reason": "invalid_response_body", "body": string(body)}
		return result
	}
	result.ObservedOwner = parsed.OwnerVersion

	lateThreshold := time.Duration(request.LateThresholdMS) * time.Millisecond
	if request.LateThresholdMS > 0 && elapsed > lateThreshold {
		result.Code = CodeProbeLate
		result.Detail = map[string]any{
			"elapsed_ms": result.ElapsedMS, "late_threshold_ms": request.LateThresholdMS,
		}
		return result
	}
	if request.ExpectedOwner != "" && parsed.OwnerVersion != request.ExpectedOwner {
		result.Code = CodeProbeMismatch
		result.Detail = map[string]any{"response_status": parsed.Status}
		return result
	}
	result.Code = ProbeOutcomeOK
	result.OK = true
	return result
}

type localConnTracker struct {
	mu    sync.Mutex
	conns []*trackedConn
}

func (t *localConnTracker) add(conn *trackedConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns = append(t.conns, conn)
}

func (t *localConnTracker) closeAll() {
	t.mu.Lock()
	conns := append([]*trackedConn(nil), t.conns...)
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func isTimeoutError(ctx context.Context, err error) bool {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func probeResultError(result ProbeResult) *Error {
	if result.OK {
		return nil
	}
	message := "probe failed"
	switch result.Code {
	case CodeProbeTimeout:
		message = "probe timed out; underlying connection was closed"
	case CodeProbeRefused:
		message = "probe endpoint refused connection"
	case CodeProbeMismatch:
		message = "probe owner identifier does not match ledger attribution"
	case CodeProbeLate:
		message = "probe response arrived after lateness threshold"
	}
	return NewError(result.Code, message).WithContextMap(map[string]any{
		"batch_id":               result.BatchID,
		"route_key":              result.RouteKey,
		"endpoint":               result.Endpoint,
		"expected_owner_version": result.ExpectedOwner,
		"observed_owner_version": result.ObservedOwner,
		"elapsed_ms":             result.ElapsedMS,
		"connection_closed":      result.ConnectionClosed,
		"slot_released":          result.SlotReleased,
		"detail":                 result.Detail,
	})
}
