package qos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type storedView struct {
	Queue        Queue
	Effective    EffectiveRates
	TokenView    TokenBucketView
	Consistency  ConsistencyScore
	CalcVersion  int64
	ValidThrough int64
}

func scanViewRow(scan func(dest ...any) error) (storedView, error) {
	var v storedView
	var dscp, effJSON, tokenJSON, consJSON string
	var parentNull sql.NullString
	var effectiveCIR, effectivePIR int64
	err := scan(&v.Queue.ID, &v.Queue.CIRBPS, &v.Queue.PIRBPS, &v.Queue.CBSBytes, &v.Queue.PBSBytes,
		&v.Queue.Priority, &v.Queue.Match.SrcPortMin, &v.Queue.Match.SrcPortMax, &dscp, &parentNull,
		&effectiveCIR, &effectivePIR, &effJSON, &tokenJSON, &consJSON, &v.CalcVersion, &v.ValidThrough)
	if err != nil {
		return v, err
	}
	v.Queue.ParentID = nil
	if parentNull.Valid {
		p := parentNull.String
		v.Queue.ParentID = &p
	}
	if err := json.Unmarshal([]byte(dscp), &v.Queue.Match.DSCPValues); err != nil {
		return v, err
	}
	if err := json.Unmarshal([]byte(effJSON), &v.Effective); err != nil {
		return v, err
	}
	if err := json.Unmarshal([]byte(tokenJSON), &v.TokenView); err != nil {
		return v, err
	}
	if err := json.Unmarshal([]byte(consJSON), &v.Consistency); err != nil {
		return v, err
	}
	// An unaffected view retains its original calculation version but is
	// explicitly extended to the current complete version.
	v.Effective.CalculationVersion = v.CalcVersion
	v.Effective.ValidThroughVersion = v.ValidThrough
	v.TokenView.CalculationVersion = v.CalcVersion
	v.TokenView.ValidThroughVersion = v.ValidThrough
	v.Consistency.CalculationVersion = v.CalcVersion
	v.Consistency.ValidThroughVersion = v.ValidThrough
	return v, nil
}

const viewSelect = `SELECT q.queue_id,q.cir_bps,q.pir_bps,q.cbs_bytes,q.pbs_bytes,q.priority,
 q.src_port_min,q.src_port_max,q.dscp_json,q.parent_id,
 v.effective_cir_bps,v.effective_pir_bps,v.effective_json,v.token_view_json,v.consistency_json,
 v.calculation_version,v.valid_through_version
 FROM queues q JOIN queue_views v ON v.queue_id=q.queue_id`

func loadOneView(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (storedView, error) {
	row := q.QueryRowContext(ctx, viewSelect+` WHERE q.queue_id=?`, id)
	return scanViewRow(row.Scan)
}

func loadAllViews(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]storedView, error) {
	rows, err := q.QueryContext(ctx, viewSelect+` ORDER BY q.queue_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storedView
	for rows.Next() {
		v, err := scanViewRow(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) readTx(ctx context.Context) (*sql.Tx, int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	current, err := currentVersionTx(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return nil, 0, err
	}
	return tx, current, nil
}

func (s *Store) GetQueueView(ctx context.Context, queueID string, observed int64) (*QueueViewResponse, *BusinessError) {
	if observed <= 0 {
		return nil, policyInvalid("observe_version must be a positive committed version", map[string]any{"observe_version": observed})
	}
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin read transaction", 500, map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	if current == 0 {
		return nil, NewError(CodeNoPolicy, "no policy has been committed", 404, map[string]any{"observed_version": observed})
	}
	view, err := loadOneView(ctx, tx, queueID)
	if errors.Is(err, sql.ErrNoRows) {
		var exists int
		checkErr := tx.QueryRowContext(ctx, `SELECT 1 FROM queues WHERE queue_id=?`, queueID).Scan(&exists)
		if checkErr == nil {
			return nil, staleRead(observed, 0, current, "derived_view_missing", map[string]any{"queue_id": queueID})
		}
		if errors.Is(checkErr, sql.ErrNoRows) {
			return nil, NewError(CodeNotFound, "queue does not exist", 404, map[string]any{"queue_id": queueID, "current_version": current})
		}
		return nil, NewError(CodeInternal, "cannot check queue existence", 500, map[string]any{"error": checkErr.Error()})
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read queue view", 500, map[string]any{"error": err.Error()})
	}
	if observed != current {
		return nil, staleRead(observed, view.CalcVersion, current, "observed_version_replaced", map[string]any{"queue_id": queueID})
	}
	if view.CalcVersion > observed || view.ValidThrough < observed {
		return nil, staleRead(observed, view.CalcVersion, current, "derived_view_lags_observed_version", map[string]any{
			"queue_id": queueID, "valid_through_version": view.ValidThrough,
		})
	}
	if err := validateViewRegistry(ctx, tx, queueID); err != nil {
		return nil, err
	}
	resp := &QueueViewResponse{
		Code: CodeOK, Version: current, UsedVersion: current,
		Queue: view.Queue, EffectiveRates: view.Effective, TokenBucketView: view.TokenView,
		Consistency: view.Consistency, CalculationVersion: view.CalcVersion, ValidThroughVersion: view.ValidThrough,
	}
	return resp, nil
}

func validateViewRegistry(ctx context.Context, tx *sql.Tx, queueID string) *BusinessError {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM queue_views WHERE queue_id=?`, queueID).Scan(&one)
	if err != nil {
		return NewError(CodeInternal, "queue view registry is incomplete", 500, map[string]any{"queue_id": queueID, "error": err.Error()})
	}
	return nil
}

func (s *Store) ListQueueViews(ctx context.Context, observed int64) (*PolicySnapshotResponse, *BusinessError) {
	if observed <= 0 {
		return nil, policyInvalid("observe_version must be a positive committed version", map[string]any{"observe_version": observed})
	}
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin read transaction", 500, map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	if current == 0 {
		return nil, NewError(CodeNoPolicy, "no policy has been committed", 404, map[string]any{"observed_version": observed})
	}
	views, err := loadAllViews(ctx, tx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read policy views", 500, map[string]any{"error": err.Error()})
	}
	var queueCount, viewCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queues`).Scan(&queueCount); err != nil {
		return nil, NewError(CodeInternal, "cannot count queues", 500, map[string]any{"error": err.Error()})
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queue_views`).Scan(&viewCount); err != nil {
		return nil, NewError(CodeInternal, "cannot count queue views", 500, map[string]any{"error": err.Error()})
	}
	if queueCount != viewCount {
		return nil, staleRead(observed, 0, current, "derived_view_registry_incomplete", map[string]any{
			"queue_count": queueCount, "view_count": viewCount,
		})
	}
	if observed != current {
		vv := int64(0)
		if len(views) > 0 {
			vv = views[0].CalcVersion
		}
		return nil, staleRead(observed, vv, current, "observed_version_replaced", nil)
	}
	policy := make(map[string]Queue, len(views))
	items := make([]QueueViewResponse, 0, len(views))
	for _, v := range views {
		if v.ValidThrough < observed || v.CalcVersion > observed {
			return nil, staleRead(observed, v.CalcVersion, current, "derived_view_lags_observed_version", map[string]any{
				"queue_id": v.Queue.ID, "valid_through_version": v.ValidThrough,
			})
		}
		policy[v.Queue.ID] = v.Queue
		items = append(items, QueueViewResponse{
			Code: CodeOK, Version: current, UsedVersion: current, Queue: v.Queue,
			EffectiveRates: v.Effective, TokenBucketView: v.TokenView, Consistency: v.Consistency,
			CalculationVersion: v.CalcVersion, ValidThroughVersion: v.ValidThrough,
		})
	}
	return &PolicySnapshotResponse{
		Code: CodeOK, Version: current, UsedVersion: current, Queues: items,
		PolicyDigest: PolicyDigest(policy),
	}, nil
}

func (s *Store) GetIntent(ctx context.Context, requestID string) (*IntentStatus, *BusinessError) {
	it, err := s.replayIntent(ctx, requestID)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read request status", 500, map[string]any{"error": err.Error()})
	}
	if it == nil {
		return nil, NewError(CodeNotFound, "request_id does not exist", 404, map[string]any{"request_id": requestID})
	}
	out := &IntentStatus{
		RequestID: it.RequestID, Kind: it.Kind, Status: it.Status, BaseVersion: it.BaseVersion,
		TargetVersion: it.TargetVersion, Phase: it.Phase, CommittedVersion: it.CommittedVersion,
		CreatedAt: it.CreatedAt, CompletedAt: it.CompletedAt,
	}
	if len(it.ResponseJSON) > 0 {
		var anyResp any
		if err := json.Unmarshal(it.ResponseJSON, &anyResp); err == nil {
			out.Response = anyResp
		}
	}
	if len(it.ErrorJSON) > 0 {
		var be BusinessError
		if err := json.Unmarshal(it.ErrorJSON, &be); err == nil {
			out.Error = &be
		}
	}
	return out, nil
}

func pendingIntents(ctx context.Context, tx *sql.Tx) ([]StoredIntent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT request_id,kind,status,base_version,target_version,phase,committed_version,
		COALESCE(response_json,''),response_status,COALESCE(error_json,''),created_at,COALESCE(completed_at,'')
		FROM intents WHERE status='pending' ORDER BY created_at,request_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredIntent
	for rows.Next() {
		var it StoredIntent
		var response, errJSON string
		if err := rows.Scan(&it.RequestID, &it.Kind, &it.Status, &it.BaseVersion, &it.TargetVersion, &it.Phase,
			&it.CommittedVersion, &response, &it.ResponseStatus, &errJSON, &it.CreatedAt, &it.CompletedAt); err != nil {
			return nil, err
		}
		it.ResponseJSON = []byte(response)
		it.ErrorJSON = []byte(errJSON)
		out = append(out, it)
	}
	return out, rows.Err()
}

func maxCommittedVersion(ctx context.Context, tx *sql.Tx) (int64, error) {
	var max sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM commits`).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

func (s *Store) RecoverPending(ctx context.Context) (*RecoveryReportResponse, *BusinessError) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin recovery transaction", 500, map[string]any{"error": err.Error()})
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	current, err := currentVersionTx(ctx, tx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read version during recovery", 500, map[string]any{"error": err.Error()})
	}
	maxCommitted, err := maxCommittedVersion(ctx, tx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read last complete version", 500, map[string]any{"error": err.Error()})
	}
	if current > maxCommitted {
		current = maxCommitted
		if err := setCurrentVersionTx(ctx, tx, current); err != nil {
			return nil, NewError(CodeInternal, "cannot restore last complete version", 500, map[string]any{"error": err.Error()})
		}
	}
	pending, err := pendingIntents(ctx, tx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot scan pending intents", 500, map[string]any{"error": err.Error()})
	}
	reports := make([]RecoveryReportEntry, 0, len(pending))
	now := nowRFC3339()
	for _, it := range pending {
		be := NewError(CodeAborted, "unfinished write was rolled back during crash recovery", 409, map[string]any{
			"request_id": it.RequestID, "kind": it.Kind, "phase": it.Phase, "rollback_version": current,
		})
		errBytes, _ := json.Marshal(be)
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted',phase='aborted',response_status=409,
			error_json=?,updated_at=?,completed_at=? WHERE request_id=? AND status='pending'`,
			string(errBytes), now, now, it.RequestID); err != nil {
			return nil, NewError(CodeInternal, "cannot mark pending intent aborted", 500, map[string]any{"error": err.Error()})
		}
		reports = append(reports, RecoveryReportEntry{
			RequestID: it.RequestID, Kind: it.Kind, Phase: it.Phase, Status: "ABORTED",
			RollbackVersion: current, DerivedRollback: true, TokenRollback: it.Kind == "simulation",
			Message: "uncommitted policy/derived/simulation state rolled back to the last complete version",
		})
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM queue_view_history WHERE version > ?`, maxCommitted); err != nil {
		return nil, NewError(CodeInternal, "cannot clean derived history", 500, map[string]any{"error": err.Error()})
	}
	// All simulation state is written in one transaction. Future-version rows are
	// defensive cleanup for databases produced by interrupted older binaries.
	if _, err := tx.ExecContext(ctx, `DELETE FROM simulation_packets WHERE simulation_id IN
		(SELECT simulation_id FROM simulations WHERE policy_version > ?)`, maxCommitted); err != nil {
		return nil, NewError(CodeInternal, "cannot clean simulation packets", 500, map[string]any{"error": err.Error()})
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM simulation_results WHERE simulation_id IN
		(SELECT simulation_id FROM simulations WHERE policy_version > ?)`, maxCommitted); err != nil {
		return nil, NewError(CodeInternal, "cannot clean simulation summaries", 500, map[string]any{"error": err.Error()})
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM simulations WHERE policy_version > ?`, maxCommitted); err != nil {
		return nil, NewError(CodeInternal, "cannot clean simulation state", 500, map[string]any{"error": err.Error()})
	}
	report := RecoveryReportResponse{
		Code: CodeOK, CurrentVersion: current, PendingIntents: len(reports), Reports: reports, CheckedAt: now,
	}
	encoded, _ := json.Marshal(reports)
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_runs(checked_at,current_version,pending_count,reports_json)
		VALUES(?,?,?,?)`, now, current, len(reports), string(encoded)); err != nil {
		return nil, NewError(CodeInternal, "cannot persist recovery report", 500, map[string]any{"error": err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return nil, NewError(CodeInternal, "recovery commit failed", 500, map[string]any{"error": err.Error()})
	}
	rollback = false
	return &report, nil
}

func (s *Store) GetRecoveryReport(ctx context.Context) (*RecoveryReportResponse, *BusinessError) {
	tx, current, err := s.readTx(ctx)
	if err != nil {
		return nil, NewError(CodeInternal, "cannot begin recovery report read", 500, map[string]any{"error": err.Error()})
	}
	defer tx.Rollback()
	var checkedAt, reportsJSON string
	var storedVersion int64
	var pendingCount int
	err = tx.QueryRowContext(ctx, `SELECT checked_at,current_version,pending_count,reports_json
		FROM recovery_runs ORDER BY id DESC LIMIT 1`).Scan(&checkedAt, &storedVersion, &pendingCount, &reportsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return &RecoveryReportResponse{Code: CodeOK, CurrentVersion: current, PendingIntents: 0,
			Reports: []RecoveryReportEntry{}, CheckedAt: nowRFC3339()}, nil
	}
	if err != nil {
		return nil, NewError(CodeInternal, "cannot read recovery report", 500, map[string]any{"error": err.Error()})
	}
	var reports []RecoveryReportEntry
	if err := json.Unmarshal([]byte(reportsJSON), &reports); err != nil {
		return nil, NewError(CodeInternal, "cannot decode recovery report", 500, map[string]any{"error": err.Error()})
	}
	if reports == nil {
		reports = []RecoveryReportEntry{}
	}
	return &RecoveryReportResponse{Code: CodeOK, CurrentVersion: current, PendingIntents: pendingCount,
		Reports: reports, CheckedAt: checkedAt}, nil
}

func (s *Store) Commits(ctx context.Context) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version,kind,request_id,parent_version,created_at,
		affected_json,recomputed_json,removed_json,policy_digest FROM commits ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var version, parent int64
		var kind, req, created, affected, recomputed, removed, digest string
		if err := rows.Scan(&version, &kind, &req, &parent, &created, &affected, &recomputed, &removed, &digest); err != nil {
			return nil, err
		}
		var a, r, d []string
		_ = json.Unmarshal([]byte(affected), &a)
		_ = json.Unmarshal([]byte(recomputed), &r)
		_ = json.Unmarshal([]byte(removed), &d)
		out = append(out, map[string]any{
			"version": version, "kind": kind, "request_id": req, "parent_version": parent,
			"created_at": created, "affected_queue_ids": a, "recomputed_queue_ids": r,
			"removed_queue_ids": d, "policy_digest": digest,
		})
	}
	return out, rows.Err()
}
