package grayrouter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type CommitSnapshotInput struct {
	RequestID string   `json:"request_id"`
	Snapshot  Snapshot `json:"snapshot"`
}

type CommitDeltaInput struct {
	RequestID string `json:"request_id"`
	Delta     Delta  `json:"delta"`
}

type ModelCommitReport struct {
	ModelKey     string             `json:"model_key"`
	Action       string             `json:"action"`
	Diff         *GraphDiff         `json:"diff,omitempty"`
	Plan         MigrationPlan      `json:"plan"`
	ArtifactRows []ArtifactSnapshot `json:"artifact_rows"`
}

type ArtifactSnapshot struct {
	Artifact string `json:"artifact"`
	Action   string `json:"action"`
	Status   string `json:"status"`
	Revision string `json:"revision"`
	Checksum string `json:"checksum"`
}

type CommitReport struct {
	Kind            string              `json:"kind"`
	Version         int64               `json:"version"`
	BaseVersion     int64               `json:"base_version,omitempty"`
	RequestID       string              `json:"request_id,omitempty"`
	CommittedAtNano int64               `json:"committed_at_ns"`
	ModelCount      int                 `json:"model_count"`
	Models          []ModelCommitReport `json:"models"`
	Plans           []MigrationPlan     `json:"plans"`
	Diffs           []GraphDiff         `json:"diffs"`
	Migrate         []string            `json:"migrate"`
	Rebuild         []string            `json:"rebuild"`
}

type StoredModel struct {
	Model               ModelVersion `json:"model"`
	SnapshotVersion     int64        `json:"snapshot_version"`
	Signature           string       `json:"signature"`
	StructuralSignature string       `json:"structural_signature"`
}

func (h *Host) SubmitSnapshot(ctx context.Context, input CommitSnapshotInput) (*CommitReport, error) {
	key := input.RequestID
	output, err := h.doIdempotent(key, func() (any, error) {
		return h.commitSnapshotOnce(ctx, input)
	})
	if err != nil {
		return nil, err
	}
	return output.(*CommitReport), nil
}

func (h *Host) SubmitDelta(ctx context.Context, input CommitDeltaInput) (*CommitReport, error) {
	key := input.RequestID
	output, err := h.doIdempotent(key, func() (any, error) {
		return h.submitDeltaGated(ctx, input)
	})
	if err != nil {
		return nil, err
	}
	return output.(*CommitReport), nil
}

func (h *Host) doIdempotent(key string, fn func() (any, error)) (any, error) {
	if strings.TrimSpace(key) == "" {
		return fn()
	}
	h.idemMu.Lock()
	if call, ok := h.idemCalls[key]; ok {
		h.idemMu.Unlock()
		<-call.done
		return call.response, call.err
	}
	call := &idempotentCall{done: make(chan struct{})}
	h.idemCalls[key] = call
	h.idemMu.Unlock()

	response, err := fn()

	h.idemMu.Lock()
	delete(h.idemCalls, key)
	h.idemMu.Unlock()
	call.response = response
	call.err = err
	close(call.done)
	return response, err
}

func (h *Host) reuseIdempotent(ctx context.Context, key, requestHash string, target any) (bool, error) {
	if strings.TrimSpace(key) == "" {
		return false, nil
	}
	var storedHash, status, responseJSON, errorJSON string
	err := h.db.QueryRowContext(ctx, `SELECT request_hash,status,response_json,error_json FROM idempotent_requests WHERE request_key=?`, key).
		Scan(&storedHash, &status, &responseJSON, &errorJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, WrapInternal(err, map[string]any{"request_id": key})
	}
	if storedHash != requestHash {
		return false, NewAPIError(CodeIdempotency, "same request_id was used with different payload", map[string]any{"request_id": key})
	}
	if status == "aborted" {
		var apiErr APIError
		if json.Unmarshal([]byte(errorJSON), &apiErr) == nil && apiErr.Code != "" {
			return true, &apiErr
		}
		return true, NewAPIError(CodeRecoveryAborted, "request was aborted by recovery", map[string]any{"request_id": key})
	}
	if status == "completed" && responseJSON != "" {
		if err := json.Unmarshal([]byte(responseJSON), target); err != nil {
			return false, WrapInternal(err, map[string]any{"request_id": key})
		}
		return true, nil
	}
	return false, NewAPIError(CodeInternal, "idempotent request is already running", map[string]any{"request_id": key, "status": status})
}

func validateModels(models []ModelVersion) error {
	var validationContext []FieldError
	for _, model := range models {
		if strings.TrimSpace(model.Name) == "" || strings.TrimSpace(model.Version) == "" {
			validationContext = append(validationContext, FieldError{Field: "name/version", Reason: "model name and version are required"})
		}
		report := ValidateGraph(model.Graph)
		validationContext = append(validationContext, report.Errors...)
	}
	if len(validationContext) > 0 {
		return NewAPIError(CodeGraphInvalid, "snapshot contains invalid operator graph", map[string]any{"errors": validationContext})
	}
	return nil
}

func (h *Host) commitSnapshotOnce(ctx context.Context, input CommitSnapshotInput) (*CommitReport, error) {
	if err := validateModels(input.Snapshot.Models); err != nil {
		return nil, err
	}
	requestHash := sha256Hex(canonicalJSON(input))
	var existing CommitReport
	if reused, err := h.reuseIdempotent(ctx, input.RequestID, requestHash, &existing); err != nil {
		return nil, err
	} else if reused {
		return &existing, nil
	}
	if err := h.lockWriter(ctx); err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer h.unlockWriter()

	head, err := h.headVersion(ctx)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if input.Snapshot.Version <= head {
		return nil, NewAPIError(CodeVersionConflict, "snapshot version must be monotonically greater than head", map[string]any{"head": head, "submitted": input.Snapshot.Version})
	}
	report, err := h.prepareAndCommit(ctx, "full_snapshot", input.Snapshot.Version, head, input.RequestID, input.Snapshot.Models, true, input)
	if err != nil {
		return nil, err
	}
	h.persistIdempotent(ctx, input.RequestID, requestHash, "snapshot", report, nil)
	return report, nil
}

func (h *Host) submitDeltaGated(ctx context.Context, input CommitDeltaInput) (*CommitReport, error) {
	if err := validateModels(input.Delta.Models); err != nil {
		return nil, err
	}
	requestHash := sha256Hex(canonicalJSON(input))
	var existing CommitReport
	if reused, err := h.reuseIdempotent(ctx, input.RequestID, requestHash, &existing); err != nil {
		return nil, err
	} else if reused {
		return &existing, nil
	}

	head, err := h.headVersion(ctx)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	base := input.Delta.BaseVersion
	h.deltaMu.Lock()
	if gate, ok := h.deltaGates[base]; ok {
		h.deltaMu.Unlock()
		select {
		case <-gate.done:
			if gate.err != nil {
				return nil, NewAPIError(CodeConflictingUpdate, "another delta for the same base version was committed first", map[string]any{
					"base_version": base, "winner_report_version": safeReportVersion(gate.report), "cause": gate.err.Error(),
				})
			}
			return nil, NewAPIError(CodeConflictingUpdate, "another delta for the same base version was committed first", map[string]any{
				"base_version": base, "winner_version": gate.report.Version,
			})
		case <-ctx.Done():
			return nil, WrapInternal(ctx.Err(), nil)
		}
	}
	if base != head {
		h.deltaMu.Unlock()
		return nil, NewAPIError(CodeStaleBase, "delta base version is not the current snapshot version", map[string]any{"current_version": head, "base_version": base})
	}
	gate := &deltaGate{base: base, done: make(chan struct{})}
	h.deltaGates[base] = gate
	h.deltaMu.Unlock()
	if h.Hooks.AfterDeltaGate != nil {
		h.Hooks.AfterDeltaGate(base)
	}

	report, err := h.commitDeltaOnce(ctx, input, requestHash)
	gate.report = report
	gate.err = err
	h.deltaMu.Lock()
	delete(h.deltaGates, base)
	h.deltaMu.Unlock()
	close(gate.done)
	return report, err
}

func safeReportVersion(report *CommitReport) int64 {
	if report == nil {
		return 0
	}
	return report.Version
}

func (h *Host) commitDeltaOnce(ctx context.Context, input CommitDeltaInput, requestHash string) (*CommitReport, error) {
	if err := h.lockWriter(ctx); err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer h.unlockWriter()
	head, err := h.headVersion(ctx)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	if input.Delta.BaseVersion != head {
		code := CodeStaleBase
		if input.Delta.BaseVersion < head {
			code = CodeConflictingUpdate
		}
		return nil, NewAPIError(code, "delta base changed while waiting for the single writer", map[string]any{"current_version": head, "base_version": input.Delta.BaseVersion})
	}
	if input.Delta.Version <= head {
		return nil, NewAPIError(CodeVersionConflict, "delta version must be greater than current version", map[string]any{"current_version": head, "submitted": input.Delta.Version})
	}
	report, err := h.prepareAndCommit(ctx, "delta_commit", input.Delta.Version, input.Delta.BaseVersion, input.RequestID, input.Delta.Models, false, input)
	if err != nil {
		return nil, err
	}
	h.persistIdempotent(ctx, input.RequestID, requestHash, "delta", report, nil)
	return report, nil
}

func (h *Host) persistIdempotent(ctx context.Context, key, hash, kind string, response any, apiErr error) {
	if strings.TrimSpace(key) == "" {
		return
	}
	status := "completed"
	responseJSON := ""
	errorJSON := ""
	if apiErr != nil {
		status = "aborted"
		if business, ok := apiErr.(*APIError); ok {
			errorJSON = string(canonicalJSON(business))
		} else {
			errorJSON = string(canonicalJSON(NewAPIError(CodeInternal, apiErr.Error(), nil)))
		}
	} else {
		responseJSON = string(canonicalJSON(response))
	}
	_, _ = h.db.ExecContext(ctx, `INSERT INTO idempotent_requests
		(request_key,request_hash,kind,status,status_code,response_json,error_json,created_at_ns,completed_at_ns)
		VALUES(?,?,?,?,?, ?,?,?,?)
		ON CONFLICT(request_key) DO UPDATE SET request_hash=excluded.request_hash,status=excluded.status,
		response_json=excluded.response_json,error_json=excluded.error_json,completed_at_ns=excluded.completed_at_ns`,
		key, hash, kind, status, 200, responseJSON, errorJSON, h.nowNano(), h.nowNano())
}

func (h *Host) prepareAndCommit(ctx context.Context, kind string, version, baseVersion int64, requestID string, models []ModelVersion, replaceAll bool, payload any) (*CommitReport, error) {
	now := h.nowNano()
	report := &CommitReport{Kind: kind, Version: version, BaseVersion: baseVersion, RequestID: requestID, CommittedAtNano: now, ModelCount: len(models)}
	intentID := fmt.Sprintf("intent-%s-%d-%s", kind, version, sha256Hex(canonicalJSON(requestID))[:12])
	_, err := h.db.ExecContext(ctx, `INSERT INTO intents
		(intent_id,kind,aggregate_id,base_version,target_version,request_id,payload_json,status,created_at_ns,updated_at_ns)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		intentID, kind, fmt.Sprintf("snapshot:%d", version), baseVersion, version, requestID, string(canonicalJSON(payload)), "prepared", now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed: intents.base_version") {
			return nil, NewAPIError(CodeConflictingUpdate, "a delta intent for this base version already exists", map[string]any{"base_version": baseVersion})
		}
		return nil, WrapInternal(err, map[string]any{"stage": "write_intent"})
	}
	h.fault("after_intent")

	type preparedModel struct {
		model     ModelVersion
		old       *StoredModel
		diff      GraphDiff
		plan      MigrationPlan
		artifacts []ArtifactSnapshot
	}
	prepared := make([]preparedModel, 0, len(models))
	for _, model := range models {
		old, err := h.currentModelByName(ctx, model.Name)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, WrapInternal(err, map[string]any{"model": model.Key()})
		}
		var oldModel *ModelVersion
		if old != nil {
			oldModel = &old.Model
		}
		diff := DiffModelVersions(valueOrZero(oldModel), model)
		plan := PlanMigration(oldModel, model, diff)
		rowReport := ModelCommitReport{ModelKey: model.Key(), Action: chooseModelAction(old, model), Plan: plan}
		if old != nil {
			diffCopy := diff
			rowReport.Diff = &diffCopy
			report.Diffs = append(report.Diffs, diff)
		}
		report.Plans = append(report.Plans, plan)
		report.Models = append(report.Models, rowReport)
		for _, action := range plan.Decisions {
			if action.Action == ActionRebuild {
				report.Rebuild = append(report.Rebuild, model.Key()+":"+action.Artifact)
			}
			if action.Action == ActionMigrate {
				report.Migrate = append(report.Migrate, model.Key()+":"+action.Artifact)
			}
		}
		artifacts := buildArtifactSnapshots(model, version, plan)
		rowReport.ArtifactRows = artifacts
		report.Models[len(report.Models)-1] = rowReport
		prepared = append(prepared, preparedModel{model: model, old: old, diff: diff, plan: plan, artifacts: artifacts})
	}

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='running',updated_at_ns=? WHERE intent_id=?`, h.nowNano(), intentID); err != nil {
		return nil, WrapInternal(err, nil)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,kind,base_version,request_id,status,payload_json,model_count,created_at_ns)
		VALUES(?,?,?,?, 'staging',?,?,?)
		ON CONFLICT(version) DO UPDATE SET status='staging',payload_json=excluded.payload_json`,
		version, kind, baseVersion, requestID, string(canonicalJSON(payload)), len(models), now); err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "insert_snapshot"})
	}
	for _, item := range prepared {
		if err := stageModel(ctx, tx, version, item.model, item.diff, item.plan, item.artifacts, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "stage_commit"})
	}
	h.fault("after_stage")

	finalTx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapInternal(err, nil)
	}
	defer func() { _ = finalTx.Rollback() }()
	committedAt := h.nowNano()
	report.CommittedAtNano = committedAt
	reportJSON := string(canonicalJSON(report))
	if _, err := finalTx.ExecContext(ctx, `UPDATE snapshots SET status='committed',report_json=?,committed_at_ns=? WHERE version=?`, reportJSON, committedAt, version); err != nil {
		return nil, WrapInternal(err, nil)
	}
	if _, err := finalTx.ExecContext(ctx, `UPDATE intents SET status='completed',updated_at_ns=? WHERE intent_id=?`, committedAt, intentID); err != nil {
		return nil, WrapInternal(err, nil)
	}
	if replaceAll {
		if _, err := finalTx.ExecContext(ctx, `UPDATE gray_plans SET status='superseded',updated_at_ns=? WHERE status='ready'`, committedAt); err != nil {
			return nil, WrapInternal(err, nil)
		}
	}
	if _, err := finalTx.ExecContext(ctx, `UPDATE derived_artifacts SET status='ready',updated_at_ns=? WHERE snapshot_version=? AND status='staging'`, committedAt, version); err != nil {
		return nil, WrapInternal(err, nil)
	}
	for _, table := range []string{"routing_entries", "prepost_cache", "gray_plans"} {
		query := fmt.Sprintf(`UPDATE %s SET status='ready' WHERE snapshot_version=? AND status='staging'`, table)
		if table == "gray_plans" {
			query = `UPDATE gray_plans SET status='ready',updated_at_ns=? WHERE snapshot_version=? AND status='staging'`
			if _, err := finalTx.ExecContext(ctx, query, committedAt, version); err != nil {
				return nil, WrapInternal(err, nil)
			}
			continue
		}
		if _, err := finalTx.ExecContext(ctx, query, version); err != nil {
			return nil, WrapInternal(err, nil)
		}
	}
	if replaceAll {
		if _, err := finalTx.ExecContext(ctx, `DELETE FROM current_models`); err != nil {
			return nil, WrapInternal(err, nil)
		}
	}
	for _, item := range prepared {
		if _, err := finalTx.ExecContext(ctx, `INSERT INTO current_models(model_name,model_key,snapshot_version,updated_at_ns)
			VALUES(?,?,?,?) ON CONFLICT(model_name) DO UPDATE SET model_key=excluded.model_key,snapshot_version=excluded.snapshot_version,updated_at_ns=excluded.updated_at_ns`,
			item.model.Name, item.model.Key(), version, committedAt); err != nil {
			return nil, WrapInternal(err, nil)
		}
	}
	if _, err := finalTx.ExecContext(ctx, `UPDATE head_state SET snapshot_version=?,updated_at_ns=? WHERE id=1`, version, committedAt); err != nil {
		return nil, WrapInternal(err, nil)
	}
	if strings.TrimSpace(requestID) != "" {
		requestHash := sha256Hex(canonicalJSON(payload))
		if _, err := finalTx.ExecContext(ctx, `INSERT INTO idempotent_requests
			(request_key,request_hash,kind,status,status_code,response_json,created_at_ns,completed_at_ns)
			VALUES(?,?,?,'completed',200,?,?,?)
			ON CONFLICT(request_key) DO UPDATE SET request_hash=excluded.request_hash,status='completed',response_json=excluded.response_json,completed_at_ns=excluded.completed_at_ns`,
			requestID, requestHash, kind, reportJSON, h.nowNano(), committedAt); err != nil {
			return nil, WrapInternal(err, nil)
		}
	}
	if err := finalTx.Commit(); err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "atomic_promotion"})
	}
	return report, nil
}

func valueOrZero(old *ModelVersion) ModelVersion {
	if old == nil {
		return ModelVersion{}
	}
	return *old
}

func chooseModelAction(old *StoredModel, model ModelVersion) string {
	if old == nil {
		return "created"
	}
	if old.Signature == GraphSignature(model.Graph) {
		return "unchanged"
	}
	return "migrated"
}

func buildArtifactSnapshots(model ModelVersion, version int64, plan MigrationPlan) []ArtifactSnapshot {
	out := make([]ArtifactSnapshot, 0, len(plan.Decisions))
	for _, decision := range plan.Decisions {
		revision := GraphSignature(model.Graph)[:16]
		checksum := sha256Hex([]byte(model.Key()), []byte(decision.Artifact), []byte(decision.Action), []byte(revision))[:24]
		out = append(out, ArtifactSnapshot{Artifact: decision.Artifact, Action: decision.Action, Status: "ready", Revision: revision, Checksum: checksum})
	}
	return out
}
