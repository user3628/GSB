package grayrouter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
)

type InferRequest struct {
	RequestID       string `json:"request_id"`
	ModelKey        string `json:"model_key,omitempty"`
	RouteKey        string `json:"route_key,omitempty"`
	SnapshotVersion int64  `json:"snapshot_version,omitempty"`
	Seed            int64  `json:"seed,omitempty"`
}

type InferResponse struct {
	RequestID       string           `json:"request_id"`
	ModelKey        string           `json:"model_key"`
	ModelName       string           `json:"model_name"`
	ModelVersion    string           `json:"model_version"`
	SnapshotVersion int64            `json:"snapshot_version"`
	BindingStatus   string           `json:"binding_status"`
	BoundAtNano     int64            `json:"bound_at_ns"`
	CompletedAtNano int64            `json:"completed_at_ns,omitempty"`
	Gray            *GrayDecision    `json:"gray_decision,omitempty"`
	Result          *ExecutionResult `json:"result,omitempty"`
}

func (h *Host) Infer(ctx context.Context, req InferRequest) (*InferResponse, error) {
	if strings.TrimSpace(req.RequestID) == "" {
		req.RequestID = "auto-" + sha256Hex(canonicalJSON(req))[:20]
	}
	out, err := h.doIdempotent(req.RequestID, func() (any, error) {
		return h.inferOnce(ctx, req, false)
	})
	if err != nil {
		return nil, err
	}
	return out.(*InferResponse), nil
}

func (h *Host) BindInference(ctx context.Context, req InferRequest) (*InferResponse, error) {
	if strings.TrimSpace(req.RequestID) == "" {
		req.RequestID = "bind-" + sha256Hex(canonicalJSON(req))[:20]
	}
	out, err := h.doIdempotent("bind:"+req.RequestID, func() (any, error) {
		return h.inferOnce(ctx, req, true)
	})
	if err != nil {
		return nil, err
	}
	return out.(*InferResponse), nil
}

func (h *Host) CompleteBound(ctx context.Context, requestID string) (*InferResponse, error) {
	out, err := h.doIdempotent("complete:"+requestID, func() (any, error) {
		return h.completeBoundOnce(ctx, requestID)
	})
	if err != nil {
		return nil, err
	}
	return out.(*InferResponse), nil
}

func (h *Host) inferOnce(ctx context.Context, req InferRequest, bindOnly bool) (*InferResponse, error) {
	if existing, err := h.loadBindingResponse(ctx, req.RequestID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.BindingStatus == "completed" || bindOnly || existing.BindingStatus == "aborted" {
			if existing.BindingStatus == "aborted" {
				return nil, NewAPIError(CodeRecoveryAborted, "in-flight binding was aborted by recovery", map[string]any{"request_id": req.RequestID})
			}
			return existing, nil
		}
		return h.completeBoundOnce(ctx, req.RequestID)
	}

	if err := h.lockWriter(ctx); err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer h.unlockWriter()

	if existing, err := h.loadBindingResponse(ctx, req.RequestID); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.BindingStatus == "aborted" {
			return nil, NewAPIError(CodeRecoveryAborted, "in-flight binding was aborted by recovery", map[string]any{"request_id": req.RequestID})
		}
		if existing.BindingStatus == "bound" && !bindOnly {
			return h.completeBoundLocked(ctx, req.RequestID)
		}
		return existing, nil
	}

	head, err := h.headVersion(ctx)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	snapshotVersion := req.SnapshotVersion
	if snapshotVersion == 0 {
		snapshotVersion = head
	}
	modelKey := req.ModelKey
	var decision *GrayDecision
	if modelKey == "" {
		decision, err = h.decideGrayLocked(ctx, GrayDecisionRequest{
			RequestID: req.RequestID, RouteKey: req.RouteKey, SnapshotVersion: snapshotVersion,
		})
		if err != nil {
			return nil, err
		}
		modelKey = decision.Selected
	}
	stored, err := h.modelByKey(ctx, modelKey)
	if err != nil {
		return nil, err
	}
	now := h.nowNano()
	intentID := "intent-infer-" + sha256Hex([]byte(req.RequestID))[:24]
	payloadJSON := string(canonicalJSON(req))
	if _, err := h.db.ExecContext(ctx, `INSERT INTO intents
		(intent_id,kind,aggregate_id,target_version,request_id,payload_json,status,created_at_ns,updated_at_ns)
		VALUES(?,?,?,?,?,?, 'prepared',?,?)`,
		intentID, "inference", req.RequestID, snapshotVersion, req.RequestID, payloadJSON, now, now); err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "write_inference_intent"})
	}
	h.fault("infer_after_intent")

	grayDigest := ""
	if decision != nil {
		grayDigest = decision.Digest
	}
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='running',updated_at_ns=? WHERE intent_id=?`, now, intentID); err != nil {
		_ = tx.Rollback()
		return nil, WrapInternal(err, nil)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO inflight_bindings
		(request_id,idempotency_key,model_key,snapshot_version,gray_digest,status,bound_at_ns,updated_at_ns)
		VALUES(?,?,?,?,?, 'bound',?,?)`,
		req.RequestID, req.RequestID, stored.Model.Key(), snapshotVersion, grayDigest, now, now); err != nil {
		_ = tx.Rollback()
		return nil, WrapInternal(err, map[string]any{"stage": "insert_binding"})
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "binding_commit"})
	}
	h.fault("after_binding")

	response := &InferResponse{
		RequestID: req.RequestID, ModelKey: stored.Model.Key(), ModelName: stored.Model.Name,
		ModelVersion: stored.Model.Version, SnapshotVersion: snapshotVersion,
		BindingStatus: "bound", BoundAtNano: now, Gray: decision,
	}
	if bindOnly {
		return response, nil
	}
	return h.completeBoundLockedWithModel(ctx, req.RequestID, stored, decision)
}

func (h *Host) completeBoundOnce(ctx context.Context, requestID string) (*InferResponse, error) {
	if err := h.lockWriter(ctx); err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer h.unlockWriter()
	return h.completeBoundLocked(ctx, requestID)
}

func (h *Host) completeBoundLocked(ctx context.Context, requestID string) (*InferResponse, error) {
	resp, err := h.loadBindingResponse(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, NewAPIError(CodeNotFound, "in-flight binding not found", map[string]any{"request_id": requestID})
	}
	if resp.BindingStatus == "completed" {
		return resp, nil
	}
	if resp.BindingStatus == "aborted" {
		return nil, NewAPIError(CodeRecoveryAborted, "in-flight binding was aborted", map[string]any{"request_id": requestID})
	}
	stored, err := h.modelByKey(ctx, resp.ModelKey)
	if err != nil {
		return nil, err
	}
	return h.completeBoundLockedWithModel(ctx, requestID, stored, resp.Gray)
}

func (h *Host) completeBoundLockedWithModel(ctx context.Context, requestID string, stored *StoredModel, decision *GrayDecision) (*InferResponse, error) {
	var seed int64
	var payload InferRequest
	var payloadJSON string
	intentID := "intent-infer-" + sha256Hex([]byte(requestID))[:24]
	_ = h.db.QueryRowContext(ctx, `SELECT payload_json FROM intents WHERE intent_id=?`, intentID).Scan(&payloadJSON)
	if payloadJSON != "" {
		_ = json.Unmarshal([]byte(payloadJSON), &payload)
		seed = payload.Seed
	}
	if seed == 0 {
		seed = DefaultSeed
	}
	atomic.AddInt64(&h.InferExecutions, 1)
	result, err := ExecuteGraph(stored.Model, seed)
	if err != nil {
		errJSON := string(canonicalJSON(err))
		_, _ = h.db.ExecContext(ctx, `UPDATE inflight_bindings SET status='failed',error_json=?,updated_at_ns=? WHERE request_id=?`, errJSON, h.nowNano(), requestID)
		_, _ = h.db.ExecContext(ctx, `UPDATE intents SET status='aborted',recovery_note=?,updated_at_ns=? WHERE intent_id=?`, "execution failed", h.nowNano(), intentID)
		return nil, err
	}
	now := h.nowNano()
	response := &InferResponse{
		RequestID: requestID, ModelKey: stored.Model.Key(), ModelName: stored.Model.Name,
		ModelVersion: stored.Model.Version, SnapshotVersion: stored.SnapshotVersion,
		BindingStatus: "completed", BoundAtNano: now, CompletedAtNano: now, Gray: decision, Result: result,
	}
	var boundAt int64
	var snapshotVersion int64
	_ = h.db.QueryRowContext(ctx, `SELECT bound_at_ns,snapshot_version FROM inflight_bindings WHERE request_id=?`, requestID).Scan(&boundAt, &snapshotVersion)
	if boundAt > 0 {
		response.BoundAtNano = boundAt
	}
	if snapshotVersion > 0 {
		response.SnapshotVersion = snapshotVersion
	}
	responseJSON := string(canonicalJSON(response))
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inflight_bindings SET status='completed',result_json=?,updated_at_ns=?,completed_at_ns=? WHERE request_id=?`, responseJSON, now, now, requestID); err != nil {
		_ = tx.Rollback()
		return nil, WrapInternal(err, nil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='completed',updated_at_ns=? WHERE intent_id=?`, now, intentID); err != nil {
		_ = tx.Rollback()
		return nil, WrapInternal(err, nil)
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapInternal(err, nil)
	}
	return response, nil
}

func (h *Host) loadBindingResponse(ctx context.Context, requestID string) (*InferResponse, error) {
	var modelKey, status, resultJSON, grayDigest string
	var snapshotVersion, boundAt, completedAt int64
	err := h.db.QueryRowContext(ctx, `SELECT model_key,snapshot_version,status,result_json,gray_digest,bound_at_ns,completed_at_ns
		FROM inflight_bindings WHERE request_id=?`, requestID).
		Scan(&modelKey, &snapshotVersion, &status, &resultJSON, &grayDigest, &boundAt, &completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if status == "completed" && resultJSON != "" {
		var response InferResponse
		if err := json.Unmarshal([]byte(resultJSON), &response); err != nil {
			return nil, WrapInternal(err, nil)
		}
		return &response, nil
	}
	response := &InferResponse{
		RequestID: requestID, ModelKey: modelKey, SnapshotVersion: snapshotVersion,
		BindingStatus: status, BoundAtNano: boundAt, CompletedAtNano: completedAt,
	}
	if grayDigest != "" {
		response.Gray = &GrayDecision{Digest: grayDigest, SnapshotVersion: snapshotVersion}
	}
	return response, nil
}

func (h *Host) DecideGray(ctx context.Context, req GrayDecisionRequest) (*GrayDecision, error) {
	if req.SnapshotVersion == 0 {
		head, err := h.headVersion(ctx)
		if err != nil {
			return nil, WrapInternal(err, nil)
		}
		req.SnapshotVersion = head
	}
	if len(req.Weights) == 0 && req.Baseline == "" && req.Canary == "" && strings.TrimSpace(req.RouteKey) != "" {
		row, err := h.grayPlanRow(ctx, req.RouteKey)
		if err != nil {
			return nil, err
		}
		req.Baseline = row.baseline
		req.Canary = row.canary
		req.CanaryRate = row.rate
		if req.Salt == "" {
			req.Salt = "grayrouter-v1"
		}
	}
	return DecideGray(req)
}

type grayPlanRow struct {
	routeKey    string
	snapshot    int64
	baseline    string
	canary      string
	rate        float64
	scale       uint64
	canaryFixed uint64
}

func (h *Host) grayPlanRow(ctx context.Context, routeKey string) (*grayPlanRow, error) {
	var row grayPlanRow
	var normalizationJSON, planJSON, status string
	err := h.db.QueryRowContext(ctx, `SELECT route_key,snapshot_version,baseline_model_key,canary_model_key,canary_rate,
		scale,canary_fixed,normalization_json,plan_json,status FROM gray_plans
		WHERE route_key=? AND status='ready' ORDER BY snapshot_version DESC LIMIT 1`, routeKey).
		Scan(&row.routeKey, &row.snapshot, &row.baseline, &row.canary, &row.rate, &row.scale, &row.canaryFixed, &normalizationJSON, &planJSON, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewAPIError(CodeNotFound, "gray plan not found", map[string]any{"route_key": routeKey})
	}
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	return &row, nil
}

func (h *Host) decideGrayLocked(ctx context.Context, req GrayDecisionRequest) (*GrayDecision, error) {
	return h.DecideGray(ctx, req)
}
