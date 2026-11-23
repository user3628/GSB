package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/store"
)

type ClientStateView struct {
	core.Client
	CurrentVersion      int64  `json:"current_version"`
	CurrentStatus       string `json:"current_status"`
	LatestActiveVersion int64  `json:"latest_active_version"`
	LatestActiveStatus  string `json:"latest_active_status"`
}

type State struct {
	GeneratedAt    string                `json:"generated_at"`
	LatestVersion  int64                 `json:"latest_version"`
	Snapshots      []core.Snapshot       `json:"snapshots"`
	Clients        []ClientStateView     `json:"clients"`
	Batches        []core.Batch          `json:"batches"`
	Pushes         []core.Push           `json:"pushes"`
	Transitions    []core.PushTransition `json:"push_transitions"`
	Acks           []core.Ack            `json:"acks"`
	AckEvents      []core.AckEvent       `json:"ack_events"`
	Results        []core.Result         `json:"results"`
	Intents        []core.Intent         `json:"intents"`
	PendingIntents []core.Intent         `json:"pending_intents"`
}

type InconsistencyItem struct {
	ClientID      string           `json:"client_id"`
	PushID        string           `json:"push_id"`
	AckID         string           `json:"ack_id"`
	PickedVersion int64            `json:"picked_version"`
	AckVersion    int64            `json:"ack_version"`
	PushStatus    string           `json:"push_status"`
	ResultDigest  string           `json:"result_digest"`
	Status        string           `json:"status"`
	Reason        string           `json:"reason"`
	CanMerge      bool             `json:"can_merge_latest"`
	Mismatches    []core.FieldDiff `json:"mismatches,omitempty"`
}

type InconsistencyReport struct {
	LatestVersion     int64               `json:"latest_version"`
	Items             []InconsistencyItem `json:"items"`
	Mergeable         []string            `json:"mergeable_result_ids"`
	RecomputeRequired []string            `json:"recompute_required_result_ids"`
	Basis             []string            `json:"decision_basis"`
}

func (e *Engine) State(ctx context.Context) (State, error) {
	state := State{GeneratedAt: core.Now()}
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		if err := scanSnapshots(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanBatches(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanPushes(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanTransitions(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanAcks(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanAckEvents(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanResults(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanRecentIntents(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanPendingIntents(ctx, tx, &state); err != nil {
			return err
		}
		if err := scanClientStates(ctx, tx, &state); err != nil {
			return err
		}
		return nil
	})
	return state, err
}

func scanSnapshots(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT version,generated_at,config_json,digest,created_at FROM snapshots ORDER BY version`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var snap core.Snapshot
		var configRaw string
		if err := rows.Scan(&snap.Version, &snap.GeneratedAt, &configRaw, &snap.Digest, &snap.CreatedAt); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(configRaw), &snap.Config); err != nil {
			return err
		}
		state.Snapshots = append(state.Snapshots, snap)
		if snap.Version > state.LatestVersion {
			state.LatestVersion = snap.Version
		}
	}
	return rows.Err()
}

func scanBatches(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT batch_id,version,status,created_at,finished_at,time_budget_ms,error_code FROM batches ORDER BY created_at,batch_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var b core.Batch
		var finished sql.NullString
		if err := rows.Scan(&b.BatchID, &b.Version, &b.Status, &b.CreatedAt, &finished, &b.TimeBudgetMS, &b.ErrorCode); err != nil {
			return err
		}
		if finished.Valid {
			b.FinishedAt = &finished.String
		}
		state.Batches = append(state.Batches, b)
	}
	return rows.Err()
}

func scanPushes(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT `+pushColumns+` FROM pushes ORDER BY version,client_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanPush(rows)
		if err != nil {
			return err
		}
		state.Pushes = append(state.Pushes, p)
	}
	return rows.Err()
}

func scanTransitions(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT push_id,from_status,to_status,reason,created_at FROM push_transitions ORDER BY transition_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var transition core.PushTransition
		if err := rows.Scan(&transition.PushID, &transition.FromStatus, &transition.ToStatus, &transition.Reason, &transition.CreatedAt); err != nil {
			return err
		}
		state.Transitions = append(state.Transitions, transition)
	}
	return rows.Err()
}

func scanAcks(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT ack_id,push_id,client_id,version,content_digest,result_digest,result_json,ingress_count,source,received_at FROM acks ORDER BY received_at`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a core.Ack
		var resultRaw string
		if err := rows.Scan(&a.AckID, &a.PushID, &a.ClientID, &a.Version, &a.ContentDigest, &a.ResultDigest, &resultRaw, &a.IngressCount, &a.Source, &a.ReceivedAt); err != nil {
			return err
		}
		a.Result = store.UnmarshalMap(resultRaw)
		state.Acks = append(state.Acks, a)
	}
	return rows.Err()
}

func scanAckEvents(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,ack_id,push_id,client_id,version,disposition,reason,content_digest,result_digest,result_json,ingress_count,received_at FROM ack_events ORDER BY received_at DESC LIMIT 500`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var a core.AckEvent
		var resultRaw string
		if err := rows.Scan(&a.EventID, &a.AckID, &a.PushID, &a.ClientID, &a.Version, &a.Disposition, &a.Reason, &a.ContentDigest, &a.ResultDigest, &resultRaw, &a.IngressCount, &a.ReceivedAt); err != nil {
			return err
		}
		a.Result = store.UnmarshalMap(resultRaw)
		state.AckEvents = append(state.AckEvents, a)
	}
	return rows.Err()
}

func scanResults(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT result_id,client_id,push_id,picked_version,ack_version,result_digest,weighted_sum,normalized,decision,seed,sample_id,sample_digest,status,reason,mismatches_json,created_at FROM results ORDER BY picked_version,client_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r core.Result
		var decision int
		var mismatchRaw string
		if err := rows.Scan(&r.ResultID, &r.ClientID, &r.PushID, &r.PickedVersion, &r.AckVersion, &r.ResultDigest, &r.WeightedSum, &r.Normalized,
			&decision, &r.Seed, &r.SampleID, &r.SampleDigest, &r.Status, &r.Reason, &mismatchRaw, &r.CreatedAt); err != nil {
			return err
		}
		r.Decision = decision != 0
		_ = json.Unmarshal([]byte(mismatchRaw), &r.Mismatches)
		state.Results = append(state.Results, r)
	}
	return rows.Err()
}

func scanPendingIntents(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT intent_id,kind,aggregate_id,payload_json,status,created_at,finished_at,detail_json FROM intents WHERE status='PREPARED' ORDER BY created_at`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var in core.Intent
		var payloadRaw, detailRaw string
		var finished sql.NullString
		if err := rows.Scan(&in.IntentID, &in.Kind, &in.AggregateID, &payloadRaw, &in.Status, &in.CreatedAt, &finished, &detailRaw); err != nil {
			return err
		}
		in.Payload = store.UnmarshalMap(payloadRaw)
		in.Detail = store.UnmarshalMap(detailRaw)
		if finished.Valid {
			in.FinishedAt = &finished.String
		}
		state.PendingIntents = append(state.PendingIntents, in)
	}
	return rows.Err()
}

func scanRecentIntents(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT intent_id,kind,aggregate_id,payload_json,status,created_at,finished_at,detail_json FROM intents ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var in core.Intent
		var payloadRaw, detailRaw string
		var finished sql.NullString
		if err := rows.Scan(&in.IntentID, &in.Kind, &in.AggregateID, &payloadRaw, &in.Status, &in.CreatedAt, &finished, &detailRaw); err != nil {
			return err
		}
		in.Payload = store.UnmarshalMap(payloadRaw)
		in.Detail = store.UnmarshalMap(detailRaw)
		if finished.Valid {
			in.FinishedAt = &finished.String
		}
		state.Intents = append(state.Intents, in)
	}
	return rows.Err()
}

func scanClientStates(ctx context.Context, tx *sql.Tx, state *State) error {
	rows, err := tx.QueryContext(ctx, `SELECT client_id,keys_json,endpoint,pickup_policy,pinned_version,created_at,updated_at FROM clients ORDER BY client_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var clients []core.Client
	for rows.Next() {
		c, err := scanClient(rows)
		if err != nil {
			return err
		}
		clients = append(clients, c)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range clients {
		view := ClientStateView{Client: c, CurrentStatus: "NO_APPLIED_VERSION", LatestActiveStatus: "NONE"}
		for _, p := range state.Pushes {
			if p.ClientID != c.ClientID {
				continue
			}
			if p.Status == core.StatusApplied && p.Version > view.CurrentVersion {
				view.CurrentVersion = p.Version
				view.CurrentStatus = core.StatusApplied
			}
			if p.Status != core.StatusAborted && p.Version > view.LatestActiveVersion {
				view.LatestActiveVersion = p.Version
				view.LatestActiveStatus = p.Status
			}
		}
		state.Clients = append(state.Clients, view)
	}
	return nil
}

func (e *Engine) Inconsistencies(ctx context.Context, latestVersion int64) (InconsistencyReport, error) {
	report := InconsistencyReport{LatestVersion: latestVersion, Items: []InconsistencyItem{}, Mergeable: []string{}, RecomputeRequired: []string{}, Basis: []string{
		"result.picked_version vs latest snapshot version",
		"confirmation.version and confirmation record",
		"newer non-aborted push versions for the same client",
		"deterministic pipeline field comparisons",
	}}
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		if report.LatestVersion == 0 {
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM snapshots`).Scan(&report.LatestVersion); err != nil {
				return err
			}
		}
		pushes := map[string]core.Push{}
		prows, err := tx.QueryContext(ctx, `SELECT `+pushColumns+` FROM pushes`)
		if err != nil {
			return err
		}
		for prows.Next() {
			p, scanErr := scanPush(prows)
			if scanErr != nil {
				prows.Close()
				return scanErr
			}
			pushes[p.PushID] = p
		}
		if err := prows.Err(); err != nil {
			prows.Close()
			return err
		}
		prows.Close()
		acks := map[string]string{}
		arows, err := tx.QueryContext(ctx, `SELECT push_id,ack_id FROM acks`)
		if err != nil {
			return err
		}
		for arows.Next() {
			var pushID, ackID string
			if err := arows.Scan(&pushID, &ackID); err != nil {
				arows.Close()
				return err
			}
			acks[pushID] = ackID
		}
		if err := arows.Err(); err != nil {
			arows.Close()
			return err
		}
		arows.Close()
		rows, err := tx.QueryContext(ctx, `SELECT result_id,client_id,push_id,picked_version,ack_version,result_digest,weighted_sum,normalized,decision,seed,sample_id,sample_digest,status,reason,mismatches_json,created_at FROM results ORDER BY picked_version,client_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r core.Result
			var decision int
			var mismatchRaw string
			if err := rows.Scan(&r.ResultID, &r.ClientID, &r.PushID, &r.PickedVersion, &r.AckVersion, &r.ResultDigest, &r.WeightedSum, &r.Normalized,
				&decision, &r.Seed, &r.SampleID, &r.SampleDigest, &r.Status, &r.Reason, &mismatchRaw, &r.CreatedAt); err != nil {
				return err
			}
			r.Decision = decision != 0
			_ = json.Unmarshal([]byte(mismatchRaw), &r.Mismatches)
			push := pushes[r.PushID]
			canMerge := r.Status == "APPLIED" && r.PickedVersion == report.LatestVersion && r.AckVersion == r.PickedVersion && push.Status == core.StatusApplied
			displayStatus := r.Status
			reason := r.Reason
			if r.PickedVersion != report.LatestVersion && r.Status == "APPLIED" {
				displayStatus = "RECOMPUTE_REQUIRED"
				reason = "SUPERSEDED_BY_LATEST_VERSION"
			}
			item := InconsistencyItem{
				ClientID: r.ClientID, PushID: r.PushID, AckID: acks[r.PushID], PickedVersion: r.PickedVersion, AckVersion: r.AckVersion,
				PushStatus: push.Status, ResultDigest: r.ResultDigest, Status: displayStatus, Reason: reason,
				CanMerge: canMerge, Mismatches: r.Mismatches,
			}
			report.Items = append(report.Items, item)
			if canMerge {
				report.Mergeable = append(report.Mergeable, r.ResultID)
			} else {
				report.RecomputeRequired = append(report.RecomputeRequired, r.ResultID)
			}
		}
		return rows.Err()
	})
	return report, err
}

func (e *Engine) LatestRecovery(ctx context.Context) (core.RecoveryReport, error) {
	var report core.RecoveryReport
	var raw string
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY finished_at DESC LIMIT 1`).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			return core.NewBizError("E_RECOVERY_NOT_FOUND", "no recovery report has been persisted", nil)
		}
		return err
	})
	if err != nil {
		return report, err
	}
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return report, err
	}
	return report, nil
}
