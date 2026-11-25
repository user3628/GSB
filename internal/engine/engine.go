package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mpckpt/internal/format"
	"mpckpt/internal/model"
	"mpckpt/internal/numeric"
	"mpckpt/internal/store"
)

type Engine struct {
	mu       sync.RWMutex
	store    *store.Store
	matrix   *format.CompatibilityMatrix
	queueCap int
	queue    []WriteTask
	now      func() time.Time
}

type StrategyRequest struct {
	CommitID string                 `json:"commit_id"`
	Strategy model.StrategySnapshot `json:"strategy"`
}

type CheckpointRequest struct {
	CommitID string        `json:"commit_id"`
	Shards   []model.Shard `json:"shards"`
	Baseline []string      `json:"baseline,omitempty"`
}

type IncrementalRequest struct {
	CommitID              string                   `json:"commit_id"`
	BaseVersion           int64                    `json:"base_version"`
	GeneratedAt           string                   `json:"generated_at,omitempty"`
	LossScaleInitialFixed *int64                   `json:"loss_scale_initial_fixed,omitempty"`
	Changes               []model.IncrementalPatch `json:"changes"`
}

type ResideRequest struct {
	CommitID string      `json:"commit_id"`
	Shard    model.Shard `json:"shard"`
	Baseline []string    `json:"baseline,omitempty"`
}

type StateResponse struct {
	DatasetVersion  int64            `json:"dataset_version"`
	StrategyVersion int64            `json:"strategy_version"`
	CommitID        string           `json:"commit_id"`
	Shards          []model.Shard    `json:"shards"`
	Baseline        []string         `json:"baseline"`
	PoolUsage       map[string]int64 `json:"pool_usage"`
	GPUCapBytes     int64            `json:"gpu_cap_bytes"`
}

type PoolResponse struct {
	DatasetVersion int64            `json:"dataset_version"`
	GPUCapBytes    int64            `json:"gpu_cap_bytes"`
	Usage          map[string]int64 `json:"usage"`
	Resident       []model.Shard    `json:"resident"`
	Persisted      []model.Shard    `json:"persisted"`
	Pending        []model.Shard    `json:"pending"`
}

type PrecisionResponse struct {
	DatasetVersion int64                   `json:"dataset_version"`
	Report         numeric.PrecisionReport `json:"report"`
}

type DerivedState struct {
	ShardID                 string `json:"shard_id"`
	PrimaryDigest           string `json:"primary_digest"`
	ConsistencyOK           bool   `json:"consistency_ok"`
	PrecisionDeviationFixed int64  `json:"precision_deviation_fixed"`
	BaselineDigest          string `json:"baseline_digest"`
	RebuiltAt               string `json:"rebuilt_at"`
}

type DerivedResponse struct {
	DatasetVersion int64          `json:"dataset_version"`
	Items          []DerivedState `json:"items"`
}

type RecoveryEvent struct {
	CommitID string          `json:"commit_id"`
	Action   string          `json:"action"`
	Details  json.RawMessage `json:"details"`
	At       string          `json:"at"`
}

type WriteTask struct {
	ShardID     string     `json:"shard_id"`
	SizeBytes   int64      `json:"size_bytes"`
	Round       int64      `json:"round"`
	Priority    int64      `json:"priority"`
	DesiredPool model.Pool `json:"desired_pool"`
}

type Eviction struct {
	ShardID       string     `json:"shard_id"`
	FreedBytes    int64      `json:"freed_bytes"`
	OriginalPool  model.Pool `json:"original_pool"`
	OriginalState string     `json:"original_status"`
}

type undoState struct {
	PendingShardIDs []string `json:"pending_shard_ids,omitempty"`
	Evictions       []struct {
		ShardID string     `json:"shard_id"`
		Pool    model.Pool `json:"pool"`
		Status  string     `json:"status"`
	} `json:"evictions,omitempty"`
}

type commitStart struct {
	ReplayResponse []byte
	ReplayError    *model.BizError
}

func New(st *store.Store, matrix *format.CompatibilityMatrix, queueCap int) (*Engine, error) {
	if matrix == nil {
		matrix = format.DefaultCompatibilityMatrix()
	}
	if queueCap < 0 {
		queueCap = 0
	}
	engine := &Engine{
		store:    st,
		matrix:   matrix,
		queueCap: queueCap,
		now:      func() time.Time { return time.Now().UTC() },
	}
	if err := engine.recoverIntents(context.Background()); err != nil {
		return nil, err
	}
	if err := engine.loadPendingQueue(); err != nil {
		return nil, err
	}
	return engine, nil
}

func (e *Engine) Close() error {
	return e.store.Close()
}

func (e *Engine) StorePath() string {
	return e.store.Path
}

func (e *Engine) SubmitStrategy(req StrategyRequest) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(req.CommitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	if req.Strategy.Checksum == "" {
		req.Strategy.Checksum = req.Strategy.ComputedChecksum()
	} else if req.Strategy.Checksum != req.Strategy.ComputedChecksum() {
		return nil, model.NewError(model.CodeSpecInvalid, "strategy checksum does not match its content", map[string]any{
			"field": "checksum", "provided": req.Strategy.Checksum, "computed": req.Strategy.ComputedChecksum(),
		})
	}
	if err := model.ValidateStrategy(req.Strategy); err != nil {
		return nil, err
	}
	hash, berr := e.hashRequest(struct {
		Kind     string                 `json:"kind"`
		Strategy model.StrategySnapshot `json:"strategy"`
	}{"strategy", req.Strategy})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(req.CommitID, "STRATEGY", hash, "")
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	current, err := e.currentStrategyVersion(context.Background())
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("read current strategy version", err))
	}
	if req.Strategy.Version <= current {
		return nil, e.failure(req.CommitID, model.NewError(model.CodeStaleSnapshot, "strategy version must increase monotonically", map[string]any{
			"submitted_version": req.Strategy.Version, "current_version": current,
		}))
	}
	dataVersion, err := metaInt64(context.Background(), e.store.DB(), "current_dataset_version")
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("read current data version", err))
	}
	if dataVersion > 0 {
		shards, berr := e.loadCurrentShards(context.Background(), e.store.DB())
		if berr != nil {
			return nil, e.failure(req.CommitID, berr)
		}
		usage := int64(0)
		for _, shard := range shards {
			if shard.Status == model.StatusResident {
				usage += shard.SizeBytes
			}
		}
		if usage > req.Strategy.GPUCapBytes {
			return nil, e.failure(req.CommitID, model.NewError(model.CodePoolFull, "new GPU cap is below current resident usage", map[string]any{
				"required_bytes":      usage - req.Strategy.GPUCapBytes,
				"current_usage_bytes": usage,
				"gpu_cap_bytes":       req.Strategy.GPUCapBytes,
				"current_version":     current,
			}))
		}
	}
	response := map[string]any{
		"commit_id": req.CommitID,
		"kind":      "STRATEGY",
		"version":   req.Strategy.Version,
		"checksum":  req.Strategy.Checksum,
	}
	rawResponse, _ := json.Marshal(response)
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(req.CommitID, internalError("begin strategy transaction", err))
	}
	defer tx.Rollback()
	dataJSON, _ := json.Marshal(req.Strategy)
	_, err = tx.Exec(`INSERT INTO strategy_snapshots(
		version, generated_at, gpu_cap_bytes, write_budget_bytes, time_budget_ms,
		loss_scale_initial_fixed, loss_scale_growth, max_abs_error_fixed, max_rel_error_ppm,
		checksum, data_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.Strategy.Version, req.Strategy.GeneratedAt, req.Strategy.GPUCapBytes,
		req.Strategy.WriteBudgetBytes, req.Strategy.TimeBudgetMillis,
		req.Strategy.LossScaleInitialFixed, req.Strategy.LossScaleGrowth,
		req.Strategy.MaxAbsErrorFixed, req.Strategy.MaxRelErrorPPM,
		req.Strategy.Checksum, string(dataJSON))
	if err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("persist strategy snapshot", err))
	}
	if err := setMeta(tx, "current_strategy_version", strconv.FormatInt(req.Strategy.Version, 10)); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("update strategy version", err))
	}
	if err := completeCommitTx(tx, req.CommitID, 0, rawResponse); err != nil {
		return nil, e.failureAfterTx(tx, req.CommitID, internalError("complete strategy commit", err))
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(req.CommitID, internalError("commit strategy transaction", err))
	}
	return rawResponse, nil
}

func (e *Engine) CurrentStrategy() (model.StrategySnapshot, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	strategy, err := e.currentStrategy(context.Background())
	if err != nil {
		return model.StrategySnapshot{}, err
	}
	return strategy, nil
}

func (e *Engine) State(version int64) (StateResponse, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if version == 0 {
		current, err := metaInt64(context.Background(), e.store.DB(), "current_dataset_version")
		if err != nil {
			return StateResponse{}, internalError("read current data version", err)
		}
		version = current
	}
	if version == 0 {
		return StateResponse{}, model.NewError(model.CodeNotFound, "no checkpoint has been committed", map[string]any{"dataset_version": version})
	}
	state, err := e.loadStateAtVersion(context.Background(), e.store.DB(), version)
	if err != nil {
		return StateResponse{}, err
	}
	return state, nil
}

func (e *Engine) Pools(version int64) (PoolResponse, *model.BizError) {
	state, err := e.State(version)
	if err != nil {
		return PoolResponse{}, err
	}
	response := PoolResponse{
		DatasetVersion: state.DatasetVersion,
		GPUCapBytes:    state.GPUCapBytes,
		Usage:          state.PoolUsage,
	}
	for _, shard := range state.Shards {
		switch shard.Status {
		case model.StatusResident:
			response.Resident = append(response.Resident, shard)
		case model.StatusPending:
			response.Pending = append(response.Pending, shard)
		default:
			response.Persisted = append(response.Persisted, shard)
		}
	}
	return response, nil
}

func (e *Engine) Precision(version int64) (PrecisionResponse, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if version == 0 {
		current, err := metaInt64(context.Background(), e.store.DB(), "current_dataset_version")
		if err != nil {
			return PrecisionResponse{}, internalError("read current data version", err)
		}
		version = current
	}
	if version == 0 {
		return PrecisionResponse{}, model.NewError(model.CodeNotFound, "no precision report exists", map[string]any{"dataset_version": version})
	}
	var raw string
	err := e.store.DB().QueryRowContext(context.Background(), `SELECT report_json FROM precision_reports WHERE data_version = ?`, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return PrecisionResponse{}, model.NewError(model.CodeNotFound, "precision report does not exist for version", map[string]any{"dataset_version": version})
	}
	if err != nil {
		return PrecisionResponse{}, internalError("load precision report", err)
	}
	var report numeric.PrecisionReport
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return PrecisionResponse{}, internalError("decode precision report", err)
	}
	return PrecisionResponse{DatasetVersion: version, Report: report}, nil
}

func (e *Engine) Recovery() ([]RecoveryEvent, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rows, err := e.store.DB().QueryContext(context.Background(), `SELECT commit_id, action, details_json, created_at FROM recovery_events ORDER BY id`)
	if err != nil {
		return nil, internalError("read recovery events", err)
	}
	defer rows.Close()
	var events []RecoveryEvent
	for rows.Next() {
		var event RecoveryEvent
		var details string
		if err := rows.Scan(&event.CommitID, &event.Action, &details, &event.At); err != nil {
			return nil, internalError("scan recovery event", err)
		}
		event.Details = json.RawMessage(details)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError("iterate recovery events", err)
	}
	if events == nil {
		events = []RecoveryEvent{}
	}
	return events, nil
}

func (e *Engine) Derived(version int64) (DerivedResponse, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if version == 0 {
		current, err := metaInt64(context.Background(), e.store.DB(), "current_dataset_version")
		if err != nil {
			return DerivedResponse{}, internalError("read current data version", err)
		}
		version = current
	}
	if version == 0 {
		return DerivedResponse{}, model.NewError(model.CodeNotFound, "no derived state exists", map[string]any{"dataset_version": version})
	}
	rows, err := e.store.DB().QueryContext(context.Background(), `SELECT shard_id, primary_digest, consistency_ok, precision_deviation_fixed, baseline_digest, rebuilt_at
		FROM derived_state WHERE data_version = ? ORDER BY shard_id`, version)
	if err != nil {
		return DerivedResponse{}, internalError("read derived state", err)
	}
	defer rows.Close()
	response := DerivedResponse{DatasetVersion: version, Items: []DerivedState{}}
	for rows.Next() {
		var item DerivedState
		var consistency int
		if err := rows.Scan(&item.ShardID, &item.PrimaryDigest, &consistency, &item.PrecisionDeviationFixed, &item.BaselineDigest, &item.RebuiltAt); err != nil {
			return DerivedResponse{}, internalError("scan derived state", err)
		}
		item.ConsistencyOK = consistency != 0
		response.Items = append(response.Items, item)
	}
	if err := rows.Err(); err != nil {
		return DerivedResponse{}, internalError("iterate derived state", err)
	}
	return response, nil
}

func (e *Engine) ImportReports(version int64) (json.RawMessage, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	query := `SELECT data_version, commit_id, format, format_version, verdict, details_json, created_at FROM import_reports`
	args := []any{}
	if version != 0 {
		query += ` WHERE data_version = ?`
		args = append(args, version)
	}
	query += ` ORDER BY id`
	rows, err := e.store.DB().QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, internalError("read import reports", err)
	}
	defer rows.Close()
	reports := []map[string]any{}
	for rows.Next() {
		var dataVersion, formatVersion int64
		var commitID, formatName, verdict, details, createdAt string
		if err := rows.Scan(&dataVersion, &commitID, &formatName, &formatVersion, &verdict, &details, &createdAt); err != nil {
			return nil, internalError("scan import report", err)
		}
		reports = append(reports, map[string]any{
			"dataset_version": dataVersion, "commit_id": commitID, "format": formatName,
			"format_version": formatVersion, "verdict": verdict, "details": json.RawMessage(details), "created_at": createdAt,
		})
	}
	raw, _ := json.Marshal(map[string]any{"reports": reports})
	return raw, nil
}

func (e *Engine) CompatibilityDecisions() (json.RawMessage, *model.BizError) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rows, err := e.store.DB().QueryContext(context.Background(), `SELECT commit_id, format, format_version, verdict, details_json, created_at
		FROM compatibility_decisions ORDER BY id`)
	if err != nil {
		return nil, internalError("read compatibility decisions", err)
	}
	defer rows.Close()
	decisions := []map[string]any{}
	for rows.Next() {
		var commitID, formatName, verdict, details, createdAt string
		var formatVersion int64
		if err := rows.Scan(&commitID, &formatName, &formatVersion, &verdict, &details, &createdAt); err != nil {
			return nil, internalError("scan compatibility decision", err)
		}
		decisions = append(decisions, map[string]any{
			"commit_id": commitID, "format": formatName, "format_version": formatVersion,
			"verdict": verdict, "details": json.RawMessage(details), "created_at": createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, internalError("iterate compatibility decisions", err)
	}
	raw, _ := json.Marshal(map[string]any{"decisions": decisions})
	return raw, nil
}

func (e *Engine) hashRequest(value any) (string, *model.BizError) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", model.NewError(model.CodeInternal, "cannot canonicalize request", map[string]any{"detail": err.Error()})
	}
	return model.SHA256Hex(raw), nil
}

func (e *Engine) beginCommit(commitID, kind, requestHash, undoJSON string) (*commitStart, *model.BizError) {
	ctx := context.Background()
	var storedHash, status, response, storedError string
	err := e.store.DB().QueryRowContext(ctx, `SELECT request_hash, status, response_json, error_json FROM commits WHERE commit_id = ?`, commitID).
		Scan(&storedHash, &status, &response, &storedError)
	if errors.Is(err, sql.ErrNoRows) {
		now := e.store.Now()
		_, err = e.store.DB().ExecContext(ctx, `INSERT INTO commits(commit_id, kind, request_hash, status, undo_json, created_at, updated_at)
			VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)`, commitID, kind, requestHash, undoJSON, now, now)
		if err != nil {
			return nil, internalError("write commit intent", err)
		}
		return &commitStart{}, nil
	}
	if err != nil {
		return nil, internalError("read commit intent", err)
	}
	if storedHash != requestHash {
		return nil, model.NewError(model.CodeConflictingRequest, "commit_id was already used with different content", map[string]any{
			"commit_id": commitID, "stored_request_hash": storedHash, "submitted_request_hash": requestHash,
		})
	}
	switch status {
	case "COMPLETED":
		return &commitStart{ReplayResponse: []byte(response)}, nil
	case "FAILED":
		var biz model.BizError
		if json.Unmarshal([]byte(storedError), &biz) == nil && biz.Code != "" {
			return &commitStart{ReplayError: &biz}, nil
		}
		return &commitStart{ReplayError: model.NewError(model.CodeInternal, "stored failed commit has no error", map[string]any{"commit_id": commitID})}, nil
	case "ROLLED_BACK":
		_, err := e.store.DB().ExecContext(ctx, `UPDATE commits SET status = 'RUNNING', error_json = '', response_json = '', data_version = 0, updated_at = ? WHERE commit_id = ?`, e.store.Now(), commitID)
		if err != nil {
			return nil, internalError("retry rolled back commit", err)
		}
		return &commitStart{}, nil
	case "RUNNING":
		return nil, model.NewError(model.CodeInProgress, "commit is already running", map[string]any{"commit_id": commitID})
	default:
		return nil, model.NewError(model.CodeInternal, "unknown commit state", map[string]any{"commit_id": commitID, "status": status})
	}
}

func (e *Engine) failure(commitID string, berr *model.BizError) *model.BizError {
	if berr == nil {
		return nil
	}
	raw, _ := json.Marshal(berr)
	_, _ = e.store.DB().Exec(`UPDATE commits SET status = 'FAILED', error_json = ?, updated_at = ? WHERE commit_id = ? AND status = 'RUNNING'`,
		string(raw), e.store.Now(), commitID)
	return berr
}

func (e *Engine) failureAfterTx(tx *sql.Tx, commitID string, berr *model.BizError) *model.BizError {
	_ = tx.Rollback()
	return e.failure(commitID, berr)
}

func completeCommitTx(tx *sql.Tx, commitID string, dataVersion int64, response []byte) error {
	_, err := tx.Exec(`UPDATE commits SET status = 'COMPLETED', data_version = ?, response_json = ?, error_json = '', updated_at = ? WHERE commit_id = ?`,
		dataVersion, string(response), time.Now().UTC().Format(time.RFC3339Nano), commitID)
	return err
}

func (e *Engine) recoverIntents(ctx context.Context) error {
	rows, err := e.store.DB().QueryContext(ctx, `SELECT commit_id, undo_json FROM commits WHERE status = 'RUNNING' ORDER BY created_at`)
	if err != nil {
		return fmt.Errorf("scan running commit intents: %w", err)
	}
	type runningIntent struct {
		CommitID string
		UndoJSON string
	}
	var intents []runningIntent
	for rows.Next() {
		var intent runningIntent
		if err := rows.Scan(&intent.CommitID, &intent.UndoJSON); err != nil {
			rows.Close()
			return fmt.Errorf("scan running intent: %w", err)
		}
		intents = append(intents, intent)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, intent := range intents {
		tx, err := e.store.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		var undo undoState
		if intent.UndoJSON != "" {
			_ = json.Unmarshal([]byte(intent.UndoJSON), &undo)
		}
		rolled := map[string]any{"pending_reset": []string{}, "evictions_restored": []string{}}
		for _, shardID := range undo.PendingShardIDs {
			result, err := tx.ExecContext(ctx, `UPDATE shards SET status = 'PENDING', write_intent = '', updated_at = ? WHERE id = ? AND write_intent = ?`, e.store.Now(), shardID, intent.CommitID)
			if err != nil {
				tx.Rollback()
				return err
			}
			if affected, _ := result.RowsAffected(); affected > 0 {
				rolled["pending_reset"] = append(rolled["pending_reset"].([]string), shardID)
			}
		}
		for _, eviction := range undo.Evictions {
			result, err := tx.ExecContext(ctx, `UPDATE shards SET pool = ?, status = ?, write_intent = '', updated_at = ? WHERE id = ? AND write_intent = ?`, eviction.Pool, eviction.Status, e.store.Now(), eviction.ShardID, intent.CommitID)
			if err != nil {
				tx.Rollback()
				return err
			}
			if affected, _ := result.RowsAffected(); affected > 0 {
				rolled["evictions_restored"] = append(rolled["evictions_restored"].([]string), eviction.ShardID)
			}
		}
		details, _ := json.Marshal(rolled)
		if _, err := tx.ExecContext(ctx, `UPDATE commits SET status = 'ROLLED_BACK', error_json = ?, updated_at = ? WHERE commit_id = ? AND status = 'RUNNING'`,
			string(mustJSON(model.NewError(model.CodeInternal, "process terminated before commit completed", map[string]any{"commit_id": intent.CommitID}))), e.store.Now(), intent.CommitID); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(commit_id, action, details_json, created_at) VALUES (?, 'ROLLBACK_UNCOMMITTED', ?, ?)`,
			intent.CommitID, string(details), e.store.Now()); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) loadPendingQueue() error {
	rows, err := e.store.DB().Query(`SELECT id, size_bytes, round_no, priority, pool FROM shards WHERE status = 'PENDING' ORDER BY priority DESC, round_no ASC, id ASC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var tasks []WriteTask
	for rows.Next() {
		var task WriteTask
		if err := rows.Scan(&task.ShardID, &task.SizeBytes, &task.Round, &task.Priority, &task.DesiredPool); err != nil {
			return err
		}
		tasks = append(tasks, task)
	}
	e.queue = tasks
	return rows.Err()
}

func mustJSON(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

func internalError(operation string, err error) *model.BizError {
	return model.NewError(model.CodeInternal, operation, map[string]any{"detail": err.Error()})
}

type dbQueryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

func metaInt64(ctx context.Context, q dbQueryer, key string) (int64, error) {
	var value string
	err := q.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func setMeta(tx *sql.Tx, key, value string) error {
	_, err := tx.Exec(`INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (e *Engine) currentStrategyVersion(ctx context.Context) (int64, error) {
	return metaInt64(ctx, e.store.DB(), "current_strategy_version")
}

func (e *Engine) currentStrategy(ctx context.Context) (model.StrategySnapshot, *model.BizError) {
	version, err := e.currentStrategyVersion(ctx)
	if err != nil {
		return model.StrategySnapshot{}, internalError("read current strategy version", err)
	}
	if version == 0 {
		return model.StrategySnapshot{}, model.NewError(model.CodeSpecInvalid, "submit a strategy snapshot before committing checkpoints", map[string]any{
			"required_operation": "POST /v1/strategy",
		})
	}
	return loadStrategy(ctx, e.store.DB(), version)
}

func loadStrategy(ctx context.Context, q dbQueryer, version int64) (model.StrategySnapshot, *model.BizError) {
	var raw string
	err := q.QueryRow(`SELECT data_json FROM strategy_snapshots WHERE version = ?`, version).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return model.StrategySnapshot{}, model.NewError(model.CodeNotFound, "strategy snapshot not found", map[string]any{"strategy_version": version})
	}
	if err != nil {
		return model.StrategySnapshot{}, internalError("load strategy snapshot", err)
	}
	var strategy model.StrategySnapshot
	if err := json.Unmarshal([]byte(raw), &strategy); err != nil {
		return model.StrategySnapshot{}, internalError("decode strategy snapshot", err)
	}
	return strategy, nil
}

func (e *Engine) loadStateAtVersion(ctx context.Context, q dbQueryer, version int64) (StateResponse, *model.BizError) {
	var strategyVersion int64
	var commitID string
	err := q.QueryRow(`SELECT strategy_version, commit_id FROM data_versions WHERE version = ?`, version).Scan(&strategyVersion, &commitID)
	if errors.Is(err, sql.ErrNoRows) {
		return StateResponse{}, model.NewError(model.CodeNotFound, "dataset version not found", map[string]any{"dataset_version": version})
	}
	if err != nil {
		return StateResponse{}, internalError("load data version", err)
	}
	strategy, berr := loadStrategy(ctx, q, strategyVersion)
	if berr != nil {
		return StateResponse{}, berr
	}
	rows, err := q.Query(`SELECT shard_id, round_no, precision, size_bytes, digest, pool, last_access, source_digest, momentum_digest, priority, status, baseline_ref
		FROM shard_versions WHERE data_version = ? ORDER BY shard_id`, version)
	if err != nil {
		return StateResponse{}, internalError("load versioned shards", err)
	}
	defer rows.Close()
	var shards []model.Shard
	var baseline []string
	usage := map[string]int64{"VRAM": 0, "PERSISTED": 0}
	for rows.Next() {
		var shard model.Shard
		var baselineRef int
		if err := rows.Scan(&shard.ID, &shard.Round, &shard.Precision, &shard.SizeBytes, &shard.Digest, &shard.Pool,
			&shard.LastAccess, &shard.SourceDigest, &shard.MomentumDigest, &shard.Priority, &shard.Status, &baselineRef); err != nil {
			return StateResponse{}, internalError("scan versioned shard", err)
		}
		shards = append(shards, shard)
		if baselineRef != 0 {
			baseline = append(baseline, shard.ID)
		}
		if shard.Status != model.StatusPending {
			usage[string(shard.Pool)] += shard.SizeBytes
		}
	}
	if err := rows.Err(); err != nil {
		return StateResponse{}, internalError("iterate versioned shards", err)
	}
	sort.Strings(baseline)
	if shards == nil {
		shards = []model.Shard{}
	}
	return StateResponse{
		DatasetVersion:  version,
		StrategyVersion: strategyVersion,
		CommitID:        commitID,
		Shards:          shards,
		Baseline:        baseline,
		PoolUsage:       usage,
		GPUCapBytes:     strategy.GPUCapBytes,
	}, nil
}

func (e *Engine) loadCurrentShards(ctx context.Context, q dbQueryer) (map[string]model.Shard, *model.BizError) {
	rows, err := q.Query(`SELECT id, round_no, precision, size_bytes, digest, pool, last_access, source_digest, momentum_digest, priority, status FROM shards ORDER BY id`)
	if err != nil {
		return nil, internalError("load current shards", err)
	}
	defer rows.Close()
	shards := map[string]model.Shard{}
	for rows.Next() {
		var shard model.Shard
		if err := rows.Scan(&shard.ID, &shard.Round, &shard.Precision, &shard.SizeBytes, &shard.Digest, &shard.Pool,
			&shard.LastAccess, &shard.SourceDigest, &shard.MomentumDigest, &shard.Priority, &shard.Status); err != nil {
			return nil, internalError("scan current shard", err)
		}
		shards[shard.ID] = shard
	}
	if err := rows.Err(); err != nil {
		return nil, internalError("iterate current shards", err)
	}
	return shards, nil
}

func (e *Engine) loadCurrentBaseline(ctx context.Context, q dbQueryer) ([]string, *model.BizError) {
	version, err := metaInt64(ctx, q, "current_dataset_version")
	if err != nil {
		return nil, internalError("read current data version", err)
	}
	if version == 0 {
		return []string{}, nil
	}
	rows, err := q.Query(`SELECT shard_id FROM baseline_refs WHERE data_version = ? ORDER BY shard_id`, version)
	if err != nil {
		return nil, internalError("load baseline", err)
	}
	defer rows.Close()
	var baseline []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, internalError("scan baseline", err)
		}
		baseline = append(baseline, id)
	}
	return baseline, nil
}
