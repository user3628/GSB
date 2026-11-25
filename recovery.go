package grayrouter

import (
	"context"
	"database/sql"
	"errors"
)

type IntentFact struct {
	IntentID      string `json:"intent_id"`
	Kind          string `json:"kind"`
	AggregateID   string `json:"aggregate_id"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	RequestID     string `json:"request_id"`
	Status        string `json:"status"`
	CreatedAtNano int64  `json:"created_at_ns"`
	UpdatedAtNano int64  `json:"updated_at_ns"`
}

type BindingFact struct {
	RequestID       string `json:"request_id"`
	ModelKey        string `json:"model_key"`
	SnapshotVersion int64  `json:"snapshot_version"`
	PreviousStatus  string `json:"previous_status"`
}

type SnapshotFact struct {
	Version        int64  `json:"version"`
	Kind           string `json:"kind"`
	BaseVersion    int64  `json:"base_version"`
	PreviousStatus string `json:"previous_status"`
}

type RecoveryReport struct {
	RecoveredAtNano        int64          `json:"recovered_at_ns"`
	LastCompleteSnapshot   int64          `json:"last_complete_snapshot"`
	Consistent             bool           `json:"consistent"`
	AbortedIntents         []IntentFact   `json:"aborted_intents"`
	AbortedBindings        []BindingFact  `json:"aborted_bindings"`
	AbortedSnapshots       []SnapshotFact `json:"aborted_snapshots"`
	AbortedArtifactRows    int64          `json:"aborted_artifact_rows"`
	AbortedRoutingRows     int64          `json:"aborted_routing_rows"`
	AbortedPrePostRows     int64          `json:"aborted_prepost_rows"`
	AbortedGrayRows        int64          `json:"aborted_gray_rows"`
	OrphanStagingSnapshots []int64        `json:"orphan_staging_snapshots"`
	Notes                  []string       `json:"notes"`
}

func (h *Host) LatestRecoveryReport() *RecoveryReport { return h.recovery }

func (h *Host) Recover(ctx context.Context) (*RecoveryReport, error) {
	if err := h.lockWriter(ctx); err != nil {
		return nil, err
	}
	defer h.unlockWriter()
	now := h.nowNano()
	report := &RecoveryReport{RecoveredAtNano: now, Consistent: true}

	rows, err := h.db.QueryContext(ctx, `SELECT intent_id,kind,aggregate_id,base_version,target_version,request_id,status,created_at_ns,updated_at_ns
		FROM intents WHERE status IN ('prepared','running') ORDER BY created_at_ns`)
	if err != nil {
		return nil, WrapInternal(err, map[string]any{"stage": "scan_intents"})
	}
	intents := []IntentFact{}
	for rows.Next() {
		var fact IntentFact
		if err := rows.Scan(&fact.IntentID, &fact.Kind, &fact.AggregateID, &fact.BaseVersion, &fact.TargetVersion,
			&fact.RequestID, &fact.Status, &fact.CreatedAtNano, &fact.UpdatedAtNano); err != nil {
			_ = rows.Close()
			return nil, WrapInternal(err, nil)
		}
		intents = append(intents, fact)
	}
	_ = rows.Close()

	for _, intent := range intents {
		report.AbortedIntents = append(report.AbortedIntents, intent)
		if intent.Kind == "inference" {
			h.abortBinding(ctx, intent.AggregateID, report)
		} else {
			h.abortSnapshotVersion(ctx, intent.TargetVersion, report)
		}
		_, _ = h.db.ExecContext(ctx, `UPDATE intents SET status='aborted',recovery_note='process terminated before completion',updated_at_ns=? WHERE intent_id=? AND status IN ('prepared','running')`, now, intent.IntentID)
	}

	h.abortOrphanStaging(ctx, report)
	h.abortOrphanBindings(ctx, report)

	head, err := h.headVersion(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, WrapInternal(err, nil)
	}
	report.LastCompleteSnapshot = head
	var stagingCount int64
	_ = h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots WHERE status='staging'`).Scan(&stagingCount)
	var boundCount int64
	_ = h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inflight_bindings WHERE status IN ('bound','running')`).Scan(&boundCount)
	report.Consistent = stagingCount == 0 && boundCount == 0
	if stagingCount > 0 {
		report.Notes = append(report.Notes, "staging snapshots remain marked aborted for audit")
	}
	if boundCount > 0 {
		report.Notes = append(report.Notes, "unresolved bound requests remain")
	}
	h.recovery = report
	return report, nil
}

func (h *Host) abortBinding(ctx context.Context, requestID string, report *RecoveryReport) {
	var fact BindingFact
	err := h.db.QueryRowContext(ctx, `SELECT request_id,model_key,snapshot_version,status FROM inflight_bindings WHERE request_id=?`, requestID).
		Scan(&fact.RequestID, &fact.ModelKey, &fact.SnapshotVersion, &fact.PreviousStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		return
	}
	if fact.PreviousStatus == "bound" || fact.PreviousStatus == "running" {
		_, _ = h.db.ExecContext(ctx, `UPDATE inflight_bindings SET status='aborted',error_json=?,updated_at_ns=? WHERE request_id=?`,
			string(canonicalJSON(NewAPIError(CodeRecoveryAborted, "process terminated while request was bound", map[string]any{"request_id": requestID}))), h.nowNano(), requestID)
		report.AbortedBindings = append(report.AbortedBindings, fact)
	}
}

func (h *Host) abortSnapshotVersion(ctx context.Context, version int64, report *RecoveryReport) {
	var fact SnapshotFact
	err := h.db.QueryRowContext(ctx, `SELECT version,kind,base_version,status FROM snapshots WHERE version=?`, version).
		Scan(&fact.Version, &fact.Kind, &fact.BaseVersion, &fact.PreviousStatus)
	if err == nil && fact.PreviousStatus == "staging" {
		_, _ = h.db.ExecContext(ctx, `UPDATE snapshots SET status='aborted' WHERE version=? AND status='staging'`, version)
		report.AbortedSnapshots = append(report.AbortedSnapshots, fact)
	}
	h.countAbortArtifacts(ctx, version, report)
}

func (h *Host) countAbortArtifacts(ctx context.Context, version int64, report *RecoveryReport) {
	if version == 0 {
		return
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE derived_artifacts SET status='aborted',updated_at_ns=? WHERE snapshot_version=? AND status='staging'`, h.nowNano(), version); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedArtifactRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE routing_entries SET status='aborted' WHERE snapshot_version=? AND status='staging'`, version); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedRoutingRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE prepost_cache SET status='aborted' WHERE snapshot_version=? AND status='staging'`, version); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedPrePostRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE gray_plans SET status='aborted',updated_at_ns=? WHERE snapshot_version=? AND status='staging'`, h.nowNano(), version); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedGrayRows += n
	}
}

func (h *Host) abortOrphanStaging(ctx context.Context, report *RecoveryReport) {
	rows, err := h.db.QueryContext(ctx, `SELECT version FROM snapshots WHERE status='staging' ORDER BY version`)
	if err == nil {
		var versions []int64
		for rows.Next() {
			var version int64
			_ = rows.Scan(&version)
			versions = append(versions, version)
		}
		_ = rows.Close()
		for _, version := range versions {
			report.OrphanStagingSnapshots = append(report.OrphanStagingSnapshots, version)
			h.abortSnapshotVersion(ctx, version, report)
		}
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE derived_artifacts SET status='aborted',updated_at_ns=? WHERE status='staging'`, h.nowNano()); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedArtifactRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE routing_entries SET status='aborted' WHERE status='staging'`); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedRoutingRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE prepost_cache SET status='aborted' WHERE status='staging'`); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedPrePostRows += n
	}
	if result, err := h.db.ExecContext(ctx, `UPDATE gray_plans SET status='aborted',updated_at_ns=? WHERE status='staging'`, h.nowNano()); err == nil {
		n, _ := result.RowsAffected()
		report.AbortedGrayRows += n
	}
}

func (h *Host) abortOrphanBindings(ctx context.Context, report *RecoveryReport) {
	rows, err := h.db.QueryContext(ctx, `SELECT request_id,model_key,snapshot_version,status FROM inflight_bindings WHERE status IN ('bound','running')`)
	if err != nil {
		return
	}
	var facts []BindingFact
	for rows.Next() {
		var fact BindingFact
		if err := rows.Scan(&fact.RequestID, &fact.ModelKey, &fact.SnapshotVersion, &fact.PreviousStatus); err == nil {
			facts = append(facts, fact)
		}
	}
	_ = rows.Close()
	for _, fact := range facts {
		_, _ = h.db.ExecContext(ctx, `UPDATE inflight_bindings SET status='aborted',error_json=?,updated_at_ns=? WHERE request_id=?`,
			string(canonicalJSON(NewAPIError(CodeRecoveryAborted, "orphan binding aborted during recovery", nil))), h.nowNano(), fact.RequestID)
		report.AbortedBindings = append(report.AbortedBindings, fact)
	}
}
