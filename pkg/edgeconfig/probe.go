package edgeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type EndpointResponse struct {
	NodeID          string `json:"node_id"`
	AppliedDigest   string `json:"applied_digest"`
	ObservedVersion int64  `json:"observed_version"`
	EffectiveAtMS   int64  `json:"effective_at_ms"`
}

type trackedDialer struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newTrackedDialer() *trackedDialer {
	return &trackedDialer{conns: make(map[net.Conn]struct{})}
}

func (d *trackedDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	tracked := &trackedConn{Conn: conn, owner: d}
	d.mu.Lock()
	d.conns[tracked] = struct{}{}
	d.mu.Unlock()
	return tracked, nil
}

func (d *trackedDialer) closeAll() {
	d.mu.Lock()
	conns := make([]net.Conn, 0, len(d.conns))
	for conn := range d.conns {
		conns = append(conns, conn)
	}
	d.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (d *trackedDialer) remove(conn net.Conn) {
	d.mu.Lock()
	delete(d.conns, conn)
	d.mu.Unlock()
}

type trackedConn struct {
	net.Conn
	owner *trackedDialer
	once  sync.Once
}

func (c *trackedConn) Close() error {
	if c.owner != nil {
		c.owner.remove(c)
	}
	var err error
	c.once.Do(func() { err = c.Conn.Close() })
	return err
}

func (s *Service) RunProbeRound(ctx context.Context, request ProbeRequest) (*ProbeRoundResult, error) {
	requestID := request.RequestID
	if requestID == "" {
		requestID = request.RoundID
	}
	if requestID == "" {
		requestID = newRandomID("req")
	}
	if request.RoundID == "" {
		request.RoundID = newRandomID("round")
	}
	request.RequestID = requestID

	return runWrite(s, ctx, "probe_round", requestID, request, func(job *writeJob[ProbeRoundResult]) (*ProbeRoundResult, *APIError) {
		current, apiErr := currentVersionAPI(job.ctx, s.store)
		if apiErr != nil {
			return nil, apiErr
		}
		topology, err := s.LoadTopology(job.ctx)
		if err != nil {
			return nil, internalError(err)
		}
		policy := withDefaultPolicy(topology.Policy)
		timeoutMS := request.TimeoutMS
		if timeoutMS <= 0 {
			timeoutMS = policy.Probe.TimeoutMS
		}
		concurrency := request.Concurrency
		if concurrency <= 0 {
			concurrency = policy.Probe.Concurrency
		}
		if concurrency <= 0 {
			concurrency = 1
		}

		targets := topology.Nodes
		if len(request.NodeIDs) > 0 {
			wanted := make(map[string]bool, len(request.NodeIDs))
			for _, id := range request.NodeIDs {
				wanted[id] = true
			}
			targets = make([]Node, 0, len(request.NodeIDs))
			for _, node := range topology.Nodes {
				if wanted[node.ID] {
					targets = append(targets, node)
				}
			}
			if len(targets) != len(request.NodeIDs) {
				unknown := make([]string, 0)
				found := make(map[string]bool)
				for _, node := range targets {
					found[node.ID] = true
				}
				for _, id := range request.NodeIDs {
					if !found[id] {
						unknown = append(unknown, id)
					}
				}
				return nil, NewAPIError(http.StatusNotFound, CodeNotFound, "probe targets not found", map[string]any{"unknown_node_ids": unknown})
			}
		}

		expectedViews, err := loadExpectedViews(job.ctx, s.db, current)
		if err != nil {
			return nil, internalError(err)
		}
		startedAt := s.NowMS()
		semaphore := make(chan struct{}, concurrency)
		outcomes := make([]ProbeOutcome, len(targets))
		var wg sync.WaitGroup
		for i, node := range targets {
			wg.Add(1)
			go func(index int, target Node) {
				defer wg.Done()
				select {
				case semaphore <- struct{}{}:
				case <-job.ctx.Done():
					outcomes[index] = ProbeOutcome{NodeID: target.ID, Endpoint: target.Endpoint, Status: StatusFailed, ErrorCode: CodeInternal, ErrorMessage: "round canceled before acquiring slot"}
					return
				}
				defer func() { <-semaphore }()
				outcomes[index] = probeNodeOnce(job.ctx, target, expectedViews[target.ID], time.Duration(timeoutMS)*time.Millisecond)
			}(i, node)
		}
		wg.Wait()

		result := &ProbeRoundResult{
			RoundID:       request.RoundID,
			RequestID:     requestID,
			TargetVersion: current,
			StartedAtMS:   startedAt,
			CompletedAtMS: s.NowMS(),
			TimeoutMS:     timeoutMS,
			Concurrency:   concurrency,
			Outcomes:      outcomes,
		}
		overrides := make(map[string]StatusOverride, len(outcomes))
		for _, outcome := range outcomes {
			switch outcome.Status {
			case StatusConverged:
				result.ConvergedCount++
			case StatusTimeout:
				result.TimeoutCount++
			case StatusRejected:
				result.RejectedCount++
			case StatusFailed:
				result.FailedCount++
			}
			overrides[outcome.NodeID] = StatusOverride{
				Status:  outcome.Status,
				Reason:  outcome.ErrorCode,
				Matched: outcome.Converged,
			}
		}

		apiErr = job.Complete(func(tx *sql.Tx) error {
			now := s.NowMS()
			for _, outcome := range outcomes {
				if _, err := tx.ExecContext(job.ctx, `
					INSERT INTO probe_records(
						round_id, node_id, endpoint, status, http_status, applied_digest,
						observed_version, effective_at_ms, error_code, error_message,
						latency_ms, timed_out, converged, expected_version
					) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					request.RoundID, outcome.NodeID, outcome.Endpoint, outcome.Status,
					outcome.HTTPStatus, outcome.AppliedDigest, outcome.ObservedVersion,
					outcome.EffectiveAtMS, outcome.ErrorCode, outcome.ErrorMessage,
					outcome.LatencyMS, boolToInt(outcome.TimedOut), boolToInt(outcome.Converged),
					outcome.ExpectedVersion); err != nil {
					return err
				}
				if outcome.HTTPStatus > 0 && outcome.Status != StatusRejected && outcome.Status != StatusTimeout && outcome.Status != StatusFailed {
					if _, err := tx.ExecContext(job.ctx, `
						UPDATE node_convergence
						SET observed_version=?, applied_digest=?, effective_at_ms=?, last_round_id=?, updated_at_ms=?
						WHERE node_id=?`,
						outcome.ObservedVersion, outcome.AppliedDigest, outcome.EffectiveAtMS,
						request.RoundID, now, outcome.NodeID); err != nil {
						return err
					}
				}
			}
			consistency, err := refreshAndRecomputeTx(job.ctx, tx, current, request.RoundID, topology.Nodes, policy, overrides)
			if err != nil {
				return err
			}
			result.Consistency = consistency
			resultJSON, err := json.Marshal(result)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(job.ctx, `
				INSERT INTO probe_rounds(
					round_id, request_id, target_version, status, timeout_ms, concurrency,
					started_at_ms, completed_at_ms, total_count, converged_count,
					timeout_count, rejected_count, failed_count, result_json
				) VALUES(?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				request.RoundID, request.RequestID, current, timeoutMS, concurrency,
				result.StartedAtMS, result.CompletedAtMS, len(outcomes), result.ConvergedCount,
				result.TimeoutCount, result.RejectedCount, result.FailedCount, string(resultJSON))
			return err
		}, result, current)
		if apiErr != nil {
			return nil, apiErr
		}
		return result, nil
	})
}

func probeNodeOnce(parent context.Context, node Node, expected ExpectedView, timeout time.Duration) ProbeOutcome {
	start := time.Now()
	outcome := ProbeOutcome{
		NodeID:          node.ID,
		Endpoint:        node.Endpoint,
		ExpectedVersion: expected.ExpectedVersion,
	}
	dialTracker := newTrackedDialer()
	transport := &http.Transport{
		DialContext:           dialTracker.DialContext,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       time.Second,
		DisableKeepAlives:     true,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	defer transport.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.Endpoint, nil)
	if err != nil {
		outcome.Status = StatusFailed
		outcome.ErrorCode = CodeProbeFailed
		outcome.ErrorMessage = err.Error()
		outcome.LatencyMS = time.Since(start).Milliseconds()
		return outcome
	}
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		timedOut := ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded)
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			timedOut = true
		}
		cancel()
		dialTracker.closeAll()
		transport.CloseIdleConnections()
		outcome.LatencyMS = time.Since(start).Milliseconds()
		if timedOut {
			outcome.Status = StatusTimeout
			outcome.TimedOut = true
			outcome.ErrorCode = CodeProbeTimeout
			outcome.ErrorMessage = "probe exceeded independent timeout: " + timeout.String()
			return outcome
		}
		outcome.Status = StatusFailed
		outcome.ErrorCode = CodeProbeFailed
		outcome.ErrorMessage = err.Error()
		return outcome
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	outcome.HTTPStatus = resp.StatusCode
	outcome.LatencyMS = time.Since(start).Milliseconds()
	if resp.StatusCode >= 400 {
		outcome.Status = StatusRejected
		outcome.ErrorCode = CodeProbeRejected
		outcome.ErrorMessage = fmt.Sprintf("endpoint returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		return outcome
	}
	var payload EndpointResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		outcome.Status = StatusFailed
		outcome.ErrorCode = CodeProbeFailed
		outcome.ErrorMessage = "invalid endpoint response: " + err.Error()
		return outcome
	}
	outcome.AppliedDigest = payload.AppliedDigest
	outcome.ObservedVersion = payload.ObservedVersion
	outcome.EffectiveAtMS = payload.EffectiveAtMS
	if expected.ExpectedVersion > payload.ObservedVersion {
		outcome.Status = StatusStale
		outcome.ErrorCode = CodeStaleRead
		return outcome
	}
	if expected.ExpectedDigest != "" && payload.AppliedDigest == expected.ExpectedDigest {
		outcome.Status = StatusConverged
		outcome.Converged = true
		return outcome
	}
	outcome.Status = StatusDiverged
	outcome.ErrorCode = "E_DIGEST_MISMATCH"
	return outcome
}
