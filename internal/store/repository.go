package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"gatewayworkbench/internal/model"
)

// ReplaceSample 在单个事务中替换整份样本；调用方已完成校验，失败即整体不生效。
func (s *Store) ReplaceSample(ctx context.Context, sample *model.Sample) error {
	tx, intentID, err := s.BeginIntent(ctx, "", "sample_load", sample)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dispatches`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM probes`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rounds`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM instance_state`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM requests`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM instances`); err != nil {
		AbortIntent(tx)
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM meta`); err != nil {
		AbortIntent(tx)
		return err
	}
	for _, ins := range sample.Instances {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO instances(id, port, weight, zone, probe_json) VALUES(?,?,?,?,?)`,
			ins.ID, ins.Port, ins.Weight, ins.Zone, mustJSON(ins.Probe)); err != nil {
			AbortIntent(tx)
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO instance_state(instance_id, phase, success_streak, failure_streak, reason, updated_at)
			 VALUES(?,?,?,?,?,?)`,
			ins.ID, string(model.PhaseUnknown), 0, 0, "initialized", nowMillis()); err != nil {
			AbortIntent(tx)
			return err
		}
	}
	for seq, req := range sample.Trace {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO requests(seq, timestamp_millis, client_id, request_weight, request_bytes, status)
			 VALUES(?,?,?,?,?, 'PENDING')`,
			seq, req.TimestampMillis, req.ClientID, req.RequestWeight, req.RequestBytes); err != nil {
			AbortIntent(tx)
			return err
		}
	}
	return s.CommitIntent(ctx, tx, intentID)
}

// ListInstances 读取实例声明。
func (s *Store) ListInstances(ctx context.Context) ([]model.Instance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, port, weight, zone, probe_json FROM instances ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Instance
	for rows.Next() {
		var ins model.Instance
		var probeJSON string
		if err := rows.Scan(&ins.ID, &ins.Port, &ins.Weight, &ins.Zone, &probeJSON); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(probeJSON), &ins.Probe); err != nil {
			return nil, err
		}
		out = append(out, ins)
	}
	return out, nil
}

// InstanceStateRow 为实例阶段持久化行。
type InstanceStateRow struct {
	InstanceID    string
	Phase         model.Phase
	SuccessStreak int
	FailureStreak int
	Reason        string
}

func (s *Store) ListStates(ctx context.Context) ([]InstanceStateRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT instance_id, phase, success_streak, failure_streak, reason FROM instance_state ORDER BY instance_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InstanceStateRow
	for rows.Next() {
		var row InstanceStateRow
		if err := rows.Scan(&row.InstanceID, &row.Phase, &row.SuccessStreak, &row.FailureStreak, &row.Reason); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (s *Store) GetState(ctx context.Context, instanceID string) (*InstanceStateRow, error) {
	var row InstanceStateRow
	err := s.db.QueryRowContext(ctx,
		`SELECT instance_id, phase, success_streak, failure_streak, reason FROM instance_state WHERE instance_id=?`,
		instanceID).Scan(&row.InstanceID, &row.Phase, &row.SuccessStreak, &row.FailureStreak, &row.Reason)
	if err == sql.ErrNoRows {
		return nil, model.NewBizError(model.CodeNotFound, "instance not found", instanceID)
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// SaveSnapshot 写入一个完整提交版本并返回其版本号。
func (s *Store) SaveSnapshot(ctx context.Context, tx *sql.Tx, snap *model.SnapshotVersion) (int64, error) {
	res, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(reason, created_at, state_json, zone_weights_json, zone_ratios_json)
		 VALUES(?,?,?,?,?)`,
		snap.Reason, nowMillis(), mustJSON(snap.InstanceState), mustJSON(snap.ZoneWeights), mustJSON(snap.ZoneRatios))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// LatestSnapshot 读取最近完整提交版本；不存在时返回 nil。
func (s *Store) LatestSnapshot(ctx context.Context) (*model.SnapshotVersion, error) {
	var version int64
	var reason, stateJSON, weightsJSON, ratiosJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT version, reason, state_json, zone_weights_json, zone_ratios_json
		 FROM snapshots ORDER BY version DESC LIMIT 1`).
		Scan(&version, &reason, &stateJSON, &weightsJSON, &ratiosJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	snap := &model.SnapshotVersion{Version: version, Reason: reason}
	if err := json.Unmarshal([]byte(stateJSON), &snap.InstanceState); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(weightsJSON), &snap.ZoneWeights); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(ratiosJSON), &snap.ZoneRatios); err != nil {
		return nil, err
	}
	return snap, nil
}

func (s *Store) CreateRound(ctx context.Context, tx *sql.Tx, id, kind, detail string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO rounds(id, kind, status, started_at, detail_json) VALUES(?,?,'RUNNING',?,?)`,
		id, kind, nowMillis(), detail)
	return err
}

func (s *Store) FinishRound(ctx context.Context, tx *sql.Tx, id, status string) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE rounds SET status=?, finished_at=? WHERE id=?`, status, nowMillis(), id)
	return err
}

// RoundStatus 返回轮次当前状态。
func (s *Store) RoundStatus(ctx context.Context, id string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM rounds WHERE id=?`, id).Scan(&status)
	if err == sql.ErrNoRows {
		return "", model.NewBizError(model.CodeNotFound, "round not found", id)
	}
	return status, err
}

func (s *Store) InsertProbe(ctx context.Context, tx *sql.Tx, roundID string, p model.ProbeOutcome) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO probes(round_id, instance_id, success, status_code, err_code, err_msg, elapsed_ms, created_at)
		 VALUES(?,?,?,?,?,?,?,?)`,
		roundID, p.InstanceID, p.Success, p.StatusCode, p.ErrCode, p.ErrMsg, p.ElapsedMS, nowMillis())
	return err
}

// ListProbes 返回某轮探测历史。
func (s *Store) ListProbes(ctx context.Context, roundID string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT instance_id, success, status_code, err_code, err_msg, elapsed_ms
		 FROM probes WHERE round_id=? ORDER BY id`, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, errCode, errMsg string
		var success bool
		var statusCode, elapsed int
		if err := rows.Scan(&id, &success, &statusCode, &errCode, &errMsg, &elapsed); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"instance_id": id, "success": success, "status_code": statusCode,
			"err_code": errCode, "err_msg": errMsg, "elapsed_ms": elapsed,
		})
	}
	return out, nil
}

// PendingRequests 按时间戳顺序、可用区轮转不可在 SQL 层完成（区信息在实例侧），
// 这里返回全部待调度请求，由调度器排序。
func (s *Store) PendingRequests(ctx context.Context, tx *sql.Tx) ([]model.RequestRecord, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT seq, timestamp_millis, client_id, request_weight, request_bytes
		 FROM requests WHERE status='PENDING' ORDER BY timestamp_millis, seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.RequestRecord
	for rows.Next() {
		var req model.RequestRecord
		var seq int
		if err := rows.Scan(&seq, &req.TimestampMillis, &req.ClientID, &req.RequestWeight, &req.RequestBytes); err != nil {
			return nil, err
		}
		_ = seq
		out = append(out, req)
	}
	return out, nil
}

// InsertDispatch 落一条派发记录（PENDING），同一轮同一请求唯一索引保证不重复派发。
func (s *Store) InsertDispatch(ctx context.Context, tx *sql.Tx, roundID string, requestSeq int, instanceID string) (int64, error) {
	res, err := tx.ExecContext(ctx,
		`INSERT INTO dispatches(round_id, request_seq, instance_id, status, started_at)
		 VALUES(?,?,?, 'PENDING', ?)`,
		roundID, requestSeq, instanceID, nowMillis())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// MarkDispatchRunning / CompleteDispatch 更新派发生命周期。
func (s *Store) MarkDispatchRunning(ctx context.Context, dispatchID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE dispatches SET status='RUNNING' WHERE id=?`, dispatchID)
	return err
}

func (s *Store) CompleteDispatch(ctx context.Context, dispatchID int64, status string, httpStatus int, errCode, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE dispatches SET status=?, http_status=?, err_code=?, err_msg=?, finished_at=? WHERE id=?`,
		status, httpStatus, errCode, errMsg, nowMillis(), dispatchID)
	return err
}

// MarkRequestDispatched 把请求与决策依据固化。
func (s *Store) MarkRequestDispatched(ctx context.Context, tx *sql.Tx, seq int, roundID string, decision *model.Decision) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE requests SET status='DISPATCHED', round_id=?, decision_json=? WHERE seq=?`,
		roundID, mustJSON(decision), seq)
	return err
}

// RequestDecision 按请求序号读取调度决策依据。
func (s *Store) RequestDecision(ctx context.Context, seq int) (map[string]any, error) {
	var clientID, status, decisionJSON string
	var roundID sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT client_id, status, round_id, COALESCE(decision_json,'') FROM requests WHERE seq=?`, seq).
		Scan(&clientID, &status, &roundID, &decisionJSON)
	if err == sql.ErrNoRows {
		return nil, model.NewBizError(model.CodeNotFound, "request not found", "seq")
	}
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"request_seq": seq, "client_id": clientID, "status": status,
		"round_id": roundID.String,
	}
	if decisionJSON != "" {
		var decision any
		if err := json.Unmarshal([]byte(decisionJSON), &decision); err != nil {
			return nil, err
		}
		out["decision"] = decision
	}
	return out, nil
}

// ListDispatches 返回某轮派发结果。
func (s *Store) ListDispatches(ctx context.Context, roundID string) ([]map[string]any, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT request_seq, instance_id, status, http_status, err_code, err_msg
		 FROM dispatches WHERE round_id=? ORDER BY request_seq`, roundID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var seq, httpStatus int
		var instanceID, status, errCode, errMsg string
		if err := rows.Scan(&seq, &instanceID, &status, &httpStatus, &errCode, &errMsg); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"request_seq": seq, "instance_id": instanceID, "status": status,
			"http_status": httpStatus, "err_code": errCode, "err_msg": errMsg,
		})
	}
	return out, nil
}

// SetInstanceState 在事务内更新阶段与计数。
func (s *Store) SetInstanceState(ctx context.Context, tx *sql.Tx, row InstanceStateRow) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE instance_state SET phase=?, success_streak=?, failure_streak=?, reason=?, updated_at=?
		 WHERE instance_id=?`,
		row.Phase, row.SuccessStreak, row.FailureStreak, row.Reason, nowMillis(), row.InstanceID)
	return err
}

// SaveAffinity 持久化亲和比例到 meta 表。
func (s *Store) SaveAffinity(ctx context.Context, tx *sql.Tx, ratios map[string]int) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO meta(key,value) VALUES('affinity',?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`, mustJSON(ratios))
	return err
}

func (s *Store) LoadAffinity(ctx context.Context) (map[string]int, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='affinity'`).Scan(&raw)
	if err == sql.ErrNoRows {
		return map[string]int{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}
