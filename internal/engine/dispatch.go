package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"local/configmiddleware/internal/core"
)

type httpOutcome struct {
	response *core.ClientResponse
	err      error
	detail   map[string]any
}

func (e *Engine) runPushRound(ctx context.Context, jobs []dispatchJob, timeout, budget time.Duration) {
	roundCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	queue := make(chan dispatchJob, e.config.QueueCapacity)

	var workers sync.WaitGroup
	workerCount := e.config.Concurrency
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue {
				e.runOne(roundCtx, job, timeout)
			}
		}()
	}

	for _, job := range jobs {
		select {
		case queue <- job:
		case <-roundCtx.Done():
			e.failJob(job.PushID, core.NewBizError("E_PUSH_BUDGET_EXCEEDED", "round budget expired while queued", map[string]any{
				"round_budget_ms": budget.Milliseconds(),
			}))
		}
	}
	close(queue)
	workers.Wait()
}

func (e *Engine) runOne(roundCtx context.Context, job dispatchJob, timeout time.Duration) {
	select {
	case e.slots <- struct{}{}:
	case <-roundCtx.Done():
		e.failJob(job.PushID, core.NewBizError("E_PUSH_BUDGET_EXCEEDED", "round budget expired before concurrency slot", nil))
		return
	}
	if roundCtx.Err() != nil {
		<-e.slots
		e.failJob(job.PushID, core.NewBizError("E_PUSH_BUDGET_EXCEEDED", "round budget expired before push", nil))
		return
	}

	push, payload, endpoint, err := e.preparePush(job.PushID)
	if err != nil {
		<-e.slots
		e.failJob(job.PushID, err)
		return
	}
	outcome := e.sendHTTP(roundCtx, push, payload, endpoint, timeout)
	<-e.slots
	if outcome.err != nil {
		e.failJob(job.PushID, withContext(outcome.err, outcome.detail))
		return
	}
	if err := e.finishConfirmation(context.Background(), push, outcome.response, timeout); err != nil {
		e.failJob(job.PushID, err)
		return
	}
	final, _ := e.getPush(context.Background(), job.PushID)
	e.completeFlight(final, nil)
}

func (e *Engine) preparePush(pushID string) (core.Push, core.PushEnvelope, string, error) {
	var push core.Push
	var envelope core.PushEnvelope
	var endpoint string
	err := e.store.ReadTx(context.Background(), func(tx *sql.Tx) error {
		var payloadRaw, endpointRaw string
		err := tx.QueryRowContext(context.Background(), `SELECT `+pushColumns+`,payload_json,target_endpoint FROM pushes WHERE push_id=?`, pushID).
			Scan(&push.PushID, &push.BatchID, &push.ClientID, &push.Version, &push.Status, &push.ContentDigest, &push.AttemptToken,
				&push.AttemptIntentID, &push.ErrorCode, &push.ErrorMessage, new(string), &push.CreatedAt, &push.UpdatedAt, &payloadRaw, &endpointRaw)
		if err != nil {
			return err
		}
		if push.Status != core.StatusPending {
			return core.NewBizError("E_INVALID_TRANSITION", "push is not PENDING at dispatch", map[string]any{"push_id": pushID, "status": push.Status})
		}
		if err := json.Unmarshal([]byte(payloadRaw), &envelope); err != nil {
			return err
		}
		endpoint = endpointRaw
		return nil
	})
	return push, envelope, endpoint, err
}

func (e *Engine) sendHTTP(roundCtx context.Context, push core.Push, envelope core.PushEnvelope, endpoint string, timeout time.Duration) httpOutcome {
	_, err := e.store.IntentWrite(context.Background(), "PUSH_DISPATCH", push.PushID, map[string]any{
		"push_id": push.PushID, "endpoint": endpoint, "timeout_ms": timeout.Milliseconds(),
	}, func(tx *sql.Tx) (bool, error) {
		p, err := getPushTx(context.Background(), tx, push.PushID)
		if err != nil {
			return false, err
		}
		if p.Status != core.StatusPending {
			if p.Status == core.StatusPushed {
				return true, nil
			}
			return false, core.NewBizError("E_INVALID_TRANSITION", "cannot dispatch non-PENDING push", map[string]any{"push_id": p.PushID, "status": p.Status})
		}
		return true, e.transitionTx(context.Background(), tx, p, core.StatusPushed, "dispatching to client")
	})
	if err != nil {
		return httpOutcome{err: err}
	}

	pushCtx, cancel := context.WithTimeout(roundCtx, timeout)
	defer cancel()
	tracker := &connTracker{conns: map[net.Conn]struct{}{}}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: -1}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 0,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			tracker.add(conn)
			return &trackedConn{Conn: conn, tracker: tracker}, nil
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return httpOutcome{err: err}
	}
	req, err := http.NewRequestWithContext(pushCtx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return httpOutcome{err: core.NewBizError("E_PUSH_UPSTREAM", "invalid client endpoint", map[string]any{"endpoint": endpoint, "error": err.Error()})}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Push-Id", push.PushID)
	started := time.Now()
	interrupted := func() httpOutcome {
		closed := tracker.closeAll()
		cancel()
		opened := tracker.openedCount()
		detail := map[string]any{
			"endpoint": endpoint, "elapsed_ms": time.Since(started).Milliseconds(),
			"connections_opened": opened, "connections_closed": closed,
		}
		if roundCtx.Err() != nil {
			return httpOutcome{err: core.NewBizError("E_PUSH_BUDGET_EXCEEDED", "round time budget exhausted", detail), detail: detail}
		}
		detail["timeout_ms"] = timeout.Milliseconds()
		return httpOutcome{err: core.NewBizError("E_PUSH_TIMEOUT", "client push timed out and its connection was closed", detail), detail: detail}
	}
	resp, err := client.Do(req)
	if err != nil {
		closed := tracker.closeAll()
		opened := tracker.openedCount()
		cancel()
		detail := map[string]any{
			"endpoint": endpoint, "elapsed_ms": time.Since(started).Milliseconds(),
			"connections_opened": opened, "connections_closed": closed,
		}
		if roundCtx.Err() != nil {
			return httpOutcome{err: core.NewBizError("E_PUSH_BUDGET_EXCEEDED", "round time budget exhausted", detail), detail: detail}
		}
		if pushCtx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			detail["timeout_ms"] = timeout.Milliseconds()
			return httpOutcome{err: core.NewBizError("E_PUSH_TIMEOUT", "client push timed out and its connection was closed", detail), detail: detail}
		}
		if isRefused(err) {
			return httpOutcome{err: core.NewBizError("E_PUSH_REFUSED", "client endpoint refused the connection", detail), detail: detail}
		}
		detail["error"] = err.Error()
		return httpOutcome{err: core.NewBizError("E_PUSH_UPSTREAM", "client push failed", detail), detail: detail}
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if readErr != nil {
		if roundCtx.Err() != nil || pushCtx.Err() != nil || errors.Is(readErr, context.DeadlineExceeded) || isTimeout(readErr) {
			return interrupted()
		}
		return httpOutcome{err: core.NewBizError("E_CLIENT_RESPONSE_INVALID", "failed to read client response", map[string]any{"error": readErr.Error()})}
	}
	if pushCtx.Err() != nil {
		return interrupted()
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpOutcome{err: core.NewBizError("E_PUSH_UPSTREAM", "client returned non-success status", map[string]any{
			"status": resp.StatusCode, "body": trimError(string(raw)), "endpoint": endpoint,
		})}
	}
	var response core.ClientResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return httpOutcome{err: core.NewBizError("E_CLIENT_RESPONSE_INVALID", "client response is not valid JSON", map[string]any{"error": err.Error(), "body": trimError(string(raw))})}
	}
	if len(response.Acks) == 0 {
		return httpOutcome{err: core.NewBizError("E_CLIENT_RESPONSE_INVALID", "client response contains no confirmation", nil)}
	}
	return httpOutcome{response: &response}
}

func (e *Engine) finishConfirmation(ctx context.Context, push core.Push, response *core.ClientResponse, timeout time.Duration) error {
	first := response.Acks[0]
	ingress := len(response.Acks)
	for _, ack := range response.Acks[1:] {
		if ack.AckID != first.AckID || ack.PushID != first.PushID || ack.ClientID != first.ClientID ||
			ack.Version != first.Version || ack.ContentDigest != first.ContentDigest ||
			ack.ResultDigest != first.ResultDigest || !reflect.DeepEqual(ack.Result, first.Result) {
			return core.NewBizError("E_CLIENT_RESPONSE_INVALID", "client returned conflicting duplicate confirmations", map[string]any{
				"push_id": push.PushID, "first_ack_id": first.AckID, "other_ack_id": ack.AckID,
			})
		}
	}
	first.IngressCount = ingress
	first.Source = "push-response"
	if _, err := e.Acknowledge(ctx, first); err != nil {
		return err
	}
	return nil
}

func (e *Engine) failJob(pushID string, err error) {
	biz := core.BizErrorOf(err)
	_ = e.abortPush(context.Background(), pushID, biz)
	push, getErr := e.getPush(context.Background(), pushID)
	if getErr != nil {
		push = core.Push{PushID: pushID, Status: core.StatusAborted, ErrorCode: biz.Code, ErrorMessage: biz.Message, ErrorContext: biz.Context}
	}
	e.completeFlight(push, err)
}

func (e *Engine) abortPush(ctx context.Context, pushID string, biz *core.BizError) error {
	_, err := e.store.IntentWrite(ctx, "PUSH_ABORT", pushID, map[string]any{"push_id": pushID, "error": biz}, func(tx *sql.Tx) (bool, error) {
		push, err := getPushTx(ctx, tx, pushID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_PUSH_NOT_FOUND", "push not found", map[string]any{"push_id": pushID})
		}
		if err != nil {
			return false, err
		}
		if push.Status == core.StatusAborted {
			return true, nil
		}
		if push.Status == core.StatusApplied || push.Status == core.StatusAcked {
			return false, core.NewBizError("E_INVALID_TRANSITION", "cannot abort push after confirmation", map[string]any{"push_id": pushID, "status": push.Status})
		}
		if err := e.transitionTx(ctx, tx, push, core.StatusAborted, biz.Code+": "+biz.Message); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE pushes SET error_code=?,error_message=?,error_context_json=?,updated_at=? WHERE push_id=?`,
			biz.Code, biz.Message, marshal(biz.Context), core.Now(), pushID); err != nil {
			return false, err
		}
		if push.AttemptIntentID != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='FAILED',finished_at=?,detail_json=? WHERE intent_id=? AND status='PREPARED'`,
				core.Now(), marshal(map[string]any{"push_id": pushID, "error": biz}), push.AttemptIntentID); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	return err
}

func withContext(err error, detail map[string]any) error {
	biz := core.BizErrorOf(err)
	if detail == nil {
		return biz
	}
	if biz.Context == nil {
		biz.Context = map[string]any{}
	}
	for key, value := range detail {
		if _, exists := biz.Context[key]; !exists {
			biz.Context[key] = value
		}
	}
	return biz
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func isRefused(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "connection refused") || strings.Contains(text, "actively refused")
}

type connTracker struct {
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	opened int
	closed int
}

func (t *connTracker) add(conn net.Conn) {
	t.mu.Lock()
	t.conns[conn] = struct{}{}
	t.opened++
	t.mu.Unlock()
}

func (t *connTracker) closeAll() int {
	t.mu.Lock()
	conns := make([]net.Conn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
		delete(t.conns, conn)
		t.closed++
	}
	closed := t.closed
	t.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	return closed
}

func (t *connTracker) openedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.opened
}

func (t *connTracker) closeConn(conn net.Conn) {
	t.mu.Lock()
	if _, ok := t.conns[conn]; ok {
		delete(t.conns, conn)
		t.closed++
	}
	t.mu.Unlock()
}

type trackedConn struct {
	net.Conn
	tracker *connTracker
	once    sync.Once
}

func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.tracker.closeConn(c.Conn)
	})
	return err
}
