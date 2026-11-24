package httpapi

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"local/sqlitepool/internal/domain"
	"local/sqlitepool/internal/pool"
)

type acquireRequest struct {
	RequestID string `json:"request_id"`
	TimeoutMS int64  `json:"timeout_ms"`
}

type releaseRequest struct {
	RequestID string `json:"request_id"`
	ConnID    string `json:"conn_id"`
}

type queryRequest struct {
	RequestID string `json:"request_id"`
	ConnID    string `json:"conn_id"`
	SQL       string `json:"sql"`
	TimeoutMS int64  `json:"timeout_ms"`
}

type configRequest struct {
	RequestID        string `json:"request_id"`
	MaxOpen          int    `json:"max_open"`
	MaxIdle          int    `json:"max_idle"`
	MaxLifetimeMS    int64  `json:"max_lifetime_ms"`
	MaxIdleTimeMS    int64  `json:"max_idle_time_ms"`
	AcquireTimeoutMS int64  `json:"acquire_timeout_ms"`
	MaxWaiters       int    `json:"max_waiters"`
	Version          int64  `json:"version"`
}

type crashFixtureRequest struct {
	RequestID string `json:"request_id"`
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	s.mutation(w, r, "POST /v1/acquire", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req acquireRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid acquire request", nil, err), requestID)
		}
		opts := pool.AcquireOptions{RequestID: requestID}
		if req.TimeoutMS > 0 {
			opts.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond
		}
		lease, err := s.pool.Acquire(ctx, opts)
		if err != nil {
			return resultFromError(err, requestID)
		}
		return successResult(http.StatusOK, requestID, map[string]any{"lease": lease})
	})
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	s.mutation(w, r, "POST /v1/release", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req releaseRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid release request", nil, err), requestID)
		}
		if req.ConnID == "" {
			return resultFromError(domain.NewError(domain.CodeBadRequest, "conn_id is required", map[string]any{
				"field": "conn_id",
			}), requestID)
		}
		result, err := s.pool.Release(ctx, req.ConnID, requestID)
		if err != nil {
			return resultFromError(err, requestID)
		}
		return successResult(http.StatusOK, requestID, map[string]any{"release": result})
	})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.mutation(w, r, "POST /v1/query", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req queryRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid query request", nil, err), requestID)
		}
		if req.ConnID == "" || req.SQL == "" {
			return resultFromError(domain.NewError(domain.CodeBadRequest, "conn_id and sql are required", map[string]any{
				"conn_id": req.ConnID,
				"sql":     req.SQL,
			}), requestID)
		}
		opts := pool.QueryOptions{RequestID: requestID}
		if req.TimeoutMS > 0 {
			opts.Timeout = time.Duration(req.TimeoutMS) * time.Millisecond
		}
		result, err := s.pool.Query(ctx, req.ConnID, req.SQL, opts)
		if err != nil {
			return resultFromError(err, requestID)
		}
		return successResult(http.StatusOK, requestID, map[string]any{"query": result})
	})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	writeResult(w, successResult(http.StatusOK, "", map[string]any{"config": s.pool.Config()}))
}

func (s *Server) handlePutConfig(w http.ResponseWriter, r *http.Request) {
	s.mutation(w, r, "PUT /v1/config", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req configRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid config request", nil, err), requestID)
		}
		cfg := domain.Config{
			MaxOpen:          req.MaxOpen,
			MaxIdle:          req.MaxIdle,
			MaxLifetimeMS:    req.MaxLifetimeMS,
			MaxIdleTimeMS:    req.MaxIdleTimeMS,
			AcquireTimeoutMS: req.AcquireTimeoutMS,
			MaxWaiters:       req.MaxWaiters,
			Version:          req.Version,
		}
		applied, err := s.pool.Reconfigure(ctx, cfg)
		if err != nil {
			return resultFromError(err, requestID)
		}
		return successResult(http.StatusOK, requestID, map[string]any{"config": applied})
	})
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	snap := s.pool.Snapshot()
	writeResult(w, successResult(http.StatusOK, "", map[string]any{
		"stats":  snap.Stats,
		"events": snap.Events,
	}))
}

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	snap := s.pool.Snapshot()
	stateFilter := r.URL.Query().Get("state")
	connections := make([]domain.ConnectionRecord, 0, len(snap.Connections))
	for _, rec := range snap.Connections {
		if stateFilter == "" || rec.State == stateFilter {
			connections = append(connections, rec)
		}
	}
	writeResult(w, successResult(http.StatusOK, "", map[string]any{
		"snapshot_version": snap.Stats.SnapshotVersion,
		"connections":      connections,
		"idle_order":       snap.Stats.IdleOrder,
	}))
}

func (s *Server) handleRecovery(w http.ResponseWriter, _ *http.Request) {
	report, err := s.store.LatestRecoveryReport(context.Background())
	if err != nil {
		writeResult(w, resultFromError(domain.WrapError(domain.CodeNotFound, "no recovery report is available", nil, err), ""))
		return
	}
	writeResult(w, successResult(http.StatusOK, "", map[string]any{"recovery": report}))
}

func (s *Server) handleCrashFixture(w http.ResponseWriter, r *http.Request) {
	if !s.opts.AllowDebug {
		writeResult(w, faultResult(domain.CodeNotFound, "debug endpoints are disabled", nil, ""))
		return
	}
	s.mutation(w, r, "POST /v1/debug/crash-fixture", func(ctx context.Context, requestID string, body []byte) apiResult {
		var req crashFixtureRequest
		if err := decodeInto(body, &req); err != nil {
			return resultFromError(domain.WrapError(domain.CodeBadRequest, "invalid crash fixture request", nil, err), requestID)
		}
		s.debugMu.Lock()
		defer s.debugMu.Unlock()
		snap := s.pool.Snapshot()
		if len(snap.Stats.IdleOrder) == 0 {
			return resultFromError(domain.NewError(domain.CodeNotFound,
				"crash fixture requires at least one idle connection", map[string]any{"idle": snap.Stats.Idle}), requestID)
		}
		idleID := snap.Stats.IdleOrder[0]
		inUseID := ""
		for _, rec := range snap.Connections {
			if rec.State == domain.StateInUse && rec.ConnectionID != idleID {
				inUseID = rec.ConnectionID
				break
			}
		}
		if inUseID == "" {
			return resultFromError(domain.NewError(domain.CodeNotFound,
				"crash fixture requires an in-use connection", map[string]any{"in_use": snap.Stats.InUse}), requestID)
		}
		acquireIntent, acquireRec, err := s.store.PrepareAcquire(ctx, idleID, requestID+"-pending-acquire")
		if err != nil {
			return resultFromError(domain.WrapError(domain.CodeInternal, "failed to prepare pending acquire", nil, err), requestID)
		}
		releaseIntent, err := s.store.PrepareRelease(ctx, inUseID, requestID+"-pending-release",
			domain.StateAvailable, "debug_crash_fixture")
		if err != nil {
			return resultFromError(domain.WrapError(domain.CodeInternal, "failed to prepare pending release", nil, err), requestID)
		}
		return successResult(http.StatusOK, requestID, map[string]any{
			"kill_now": true,
			"pending_acquire": map[string]any{
				"intent_id":     acquireIntent,
				"connection_id": idleID,
				"durable_state": acquireRec.State,
			},
			"pending_release": map[string]any{
				"intent_id":     releaseIntent,
				"connection_id": inUseID,
				"durable_state": domain.StateAvailable,
			},
			"note": "这两个意图故意未标记 committed；立即 SIGKILL，重启后由恢复扫描回滚。",
		})
	})
}

func parseOptionalInt(v string) (int, bool) {
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}
