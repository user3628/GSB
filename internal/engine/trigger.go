package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"local/configmiddleware/internal/core"
)

type dispatchJob struct {
	PushID   string
	ClientID string
	Version  int64
}

type createBatchResult struct {
	batch      core.Batch
	newJobs    []dispatchJob
	reused     map[string]bool
	allPushIDs []string
}

func (e *Engine) TriggerPush(ctx context.Context, req TriggerRequest) (*TriggerResult, error) {
	if req.Version <= 0 {
		return nil, core.NewBizError("E_BAD_REQUEST", "version is required", nil)
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = e.config.DefaultTimeout
	}
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	budget := time.Duration(req.RoundBudgetMS) * time.Millisecond
	if budget <= 0 {
		budget = e.config.DefaultRoundBudget
	}
	if budget > 60*time.Second {
		budget = 60 * time.Second
	}

	// Holding flightMu across state creation closes the race where a duplicate
	// trigger observes PENDING before the leader has published its in-flight job.
	e.flightMu.Lock()
	created, err := e.createBatch(ctx, req, budget)
	if err != nil {
		e.flightMu.Unlock()
		return nil, err
	}
	for _, job := range created.newJobs {
		e.flights[job.PushID] = &flight{done: make(chan struct{})}
	}
	e.flightMu.Unlock()

	var wg sync.WaitGroup
	if len(created.newJobs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runPushRound(ctx, created.newJobs, timeout, budget)
		}()
	}

	// Reused pushes in PENDING/PUSHED are joined to the leader's result.
	for _, pushID := range created.allPushIDs {
		if created.reused[pushID] {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				e.waitForFlight(ctx, id)
			}(pushID)
		}
	}
	wg.Wait()

	if len(created.newJobs) > 0 || len(created.allPushIDs) > 0 {
		if finishErr := e.finishBatch(ctx, created.batch.BatchID); finishErr != nil {
			return nil, finishErr
		}
	}
	return e.triggerResult(ctx, created)
}

func (e *Engine) createBatch(ctx context.Context, req TriggerRequest, budget time.Duration) (*createBatchResult, error) {
	created := &createBatchResult{reused: map[string]bool{}}
	_, err := e.store.IntentWrite(ctx, "BATCH_CREATE", fmt.Sprintf("batch:version:%d", req.Version), req, func(tx *sql.Tx) (bool, error) {
		var snap core.Snapshot
		var configRaw string
		err := tx.QueryRowContext(ctx, `SELECT version,generated_at,config_json,digest FROM snapshots WHERE version=?`, req.Version).
			Scan(&snap.Version, &snap.GeneratedAt, &configRaw, &snap.Digest)
		if errors.Is(err, sql.ErrNoRows) {
			return false, core.NewBizError("E_SNAPSHOT_NOT_FOUND", "snapshot not found", map[string]any{"version": req.Version})
		}
		if err != nil {
			return false, err
		}
		if err := json.Unmarshal([]byte(configRaw), &snap.Config); err != nil {
			return false, err
		}
		clients, err := selectedClientsTx(ctx, tx, req.ClientIDs)
		if err != nil {
			return false, err
		}
		if len(clients) == 0 {
			return false, core.NewBizError("E_NO_CLIENTS", "no matching clients", map[string]any{"client_ids": req.ClientIDs})
		}

		batchID := core.NewID("batch")
		now := core.Now()
		created.batch = core.Batch{BatchID: batchID, Version: req.Version, Status: "OPEN", CreatedAt: now, TimeBudgetMS: budget.Milliseconds()}
		if _, err := tx.ExecContext(ctx, `INSERT INTO batches(batch_id,version,status,created_at,time_budget_ms) VALUES(?,?,?,?,?)`,
			batchID, req.Version, "OPEN", now, budget.Milliseconds()); err != nil {
			return false, err
		}

		for _, client := range clients {
			if client.PickupPolicy == core.PickupPinned && (client.PinnedVersion == nil || *client.PinnedVersion != req.Version) {
				if len(req.ClientIDs) > 0 {
					return false, core.NewBizError("E_PICKUP_POLICY_MISMATCH", "pinned client requested for a different version", map[string]any{
						"client_id": client.ClientID, "requested": req.Version, "pinned": client.PinnedVersion,
					})
				}
				continue
			}
			config, err := copyConfig(snap.Config, client.Keys)
			if err != nil {
				return false, err
			}
			contentDigest := core.PushContentDigest(client.ClientID, req.Version, snap.Digest, config)
			baseVersion, baseConfig := appliedBaseTx(ctx, tx, client.ClientID, req.Version)
			changes := map[string]float64{}
			for key, value := range config {
				old, existed := baseConfig[key]
				if baseVersion == 0 || !existed || old != value {
					changes[key] = value
				}
			}
			envelope := core.PushEnvelope{
				ClientID: client.ClientID, Version: req.Version, SnapshotDigest: snap.Digest,
				BaseVersion: baseVersion, ContentDigest: contentDigest, Config: config, Changes: changes,
				Pipeline: core.PipelineRef{Seed: e.samples.Seed, SampleID: e.samples.SampleID, SampleDigest: e.samples.Digest},
			}

			var existing core.Push
			existing, err = scanPush(tx.QueryRowContext(ctx, `SELECT `+pushColumns+` FROM pushes WHERE client_id=? AND version=?`, client.ClientID, req.Version))
			switch {
			case err == nil:
				if req.ContentDigest != "" && req.ContentDigest != existing.ContentDigest {
					return false, core.NewBizError("E_CONFLICTING_PUSH", "duplicate push content assertion differs", map[string]any{
						"client_id": client.ClientID, "version": req.Version, "existing": existing.ContentDigest, "requested": req.ContentDigest,
					})
				}
				if existing.ContentDigest != contentDigest {
					return false, core.NewBizError("E_CONFLICTING_PUSH", "duplicate push content differs from persisted push", map[string]any{
						"client_id": client.ClientID, "version": req.Version, "existing": existing.ContentDigest, "computed": contentDigest,
					})
				}
				created.reused[existing.PushID] = true
				created.allPushIDs = append(created.allPushIDs, existing.PushID)
				continue
			case !errors.Is(err, sql.ErrNoRows):
				return false, err
			}
			if req.ContentDigest != "" && req.ContentDigest != contentDigest {
				return false, core.NewBizError("E_SNAPSHOT_INVALID", "content_digest assertion does not match canonical push content", map[string]any{
					"expected": contentDigest, "actual": req.ContentDigest,
				})
			}
			pushID := core.NewID("push")
			attemptIntentID := core.NewID("intent")
			attemptToken := core.NewID("attempt")
			envelope.PushID = pushID
			envelope.BatchID = batchID
			payloadJSON := marshal(envelope)
			intentPayload := marshal(map[string]any{
				"push_id": pushID, "client_id": client.ClientID, "version": req.Version,
				"batch_id": batchID, "endpoint": client.Endpoint, "content_digest": contentDigest,
			})
			if _, err := tx.ExecContext(ctx, `INSERT INTO intents(intent_id,kind,aggregate_id,payload_json,status,created_at,detail_json) VALUES(?,?,?,?,?,?,?)`,
				attemptIntentID, "PUSH_ATTEMPT", pushID, intentPayload, "PREPARED", now, "{}"); err != nil {
				return false, err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO pushes(push_id,batch_id,client_id,version,status,content_digest,attempt_token,attempt_intent_id,payload_json,target_endpoint,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
				pushID, batchID, client.ClientID, req.Version, core.StatusPending, contentDigest, attemptToken,
				attemptIntentID, payloadJSON, client.Endpoint, now, now); err != nil {
				return false, err
			}
			if err := insertTransitionTx(ctx, tx, pushID, "", core.StatusPending, "batch created", now); err != nil {
				return false, err
			}
			created.newJobs = append(created.newJobs, dispatchJob{PushID: pushID, ClientID: client.ClientID, Version: req.Version})
			created.allPushIDs = append(created.allPushIDs, pushID)
		}
		if len(created.allPushIDs) == 0 {
			return false, core.NewBizError("E_NO_CLIENTS", "no eligible clients for this version", map[string]any{"version": req.Version})
		}
		if len(created.newJobs) == 0 {
			now2 := core.Now()
			created.batch.Status = "DISPATCHED"
			created.batch.FinishedAt = &now2
			if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='DISPATCHED',finished_at=? WHERE batch_id=?`, now2, batchID); err != nil {
				return false, err
			}
		} else {
			created.batch.Status = "DISPATCHING"
			if _, err := tx.ExecContext(ctx, `UPDATE batches SET status='DISPATCHING' WHERE batch_id=?`, batchID); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func appliedBaseTx(ctx context.Context, tx *sql.Tx, clientID string, beforeVersion int64) (int64, map[string]float64) {
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM pushes WHERE client_id=? AND status='APPLIED' AND version<?`,
		clientID, beforeVersion).Scan(&version); err != nil || version == 0 {
		return 0, nil
	}
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT config_json FROM snapshots WHERE version=?`, version).Scan(&raw); err != nil {
		return 0, nil
	}
	var config map[string]float64
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return 0, nil
	}
	return version, config
}

func selectedClientsTx(ctx context.Context, tx *sql.Tx, ids []string) ([]core.Client, error) {
	if len(ids) == 0 {
		rows, err := tx.QueryContext(ctx, `SELECT client_id,keys_json,endpoint,pickup_policy,pinned_version,created_at,updated_at FROM clients ORDER BY client_id`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var clients []core.Client
		for rows.Next() {
			c, err := scanClient(rows)
			if err != nil {
				return nil, err
			}
			clients = append(clients, c)
		}
		return clients, rows.Err()
	}
	seen := map[string]bool{}
	var clients []core.Client
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		c, err := getClientTx(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.NewBizError("E_CLIENT_NOT_FOUND", "client not found", map[string]any{"client_id": id})
		}
		if err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return clients, nil
}

func insertTransitionTx(ctx context.Context, tx *sql.Tx, pushID, from, to, reason, when string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO push_transitions(push_id,from_status,to_status,reason,created_at) VALUES(?,?,?,?,?)`,
		pushID, from, to, reason, when)
	return err
}

func (e *Engine) transitionTx(ctx context.Context, tx *sql.Tx, push core.Push, to, reason string) error {
	if !core.CanTransition(push.Status, to) {
		return core.NewBizError("E_INVALID_TRANSITION", "illegal push state transition", map[string]any{
			"push_id": push.PushID, "from": push.Status, "to": to,
		})
	}
	now := core.Now()
	result, err := tx.ExecContext(ctx, `UPDATE pushes SET status=?,updated_at=? WHERE push_id=? AND status=?`, to, now, push.PushID, push.Status)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return core.NewBizError("E_INVALID_TRANSITION", "push state changed concurrently", map[string]any{"push_id": push.PushID, "from": push.Status, "to": to})
	}
	return insertTransitionTx(ctx, tx, push.PushID, push.Status, to, reason, now)
}

func (e *Engine) waitForFlight(ctx context.Context, pushID string) core.Push {
	e.flightMu.Lock()
	f := e.flights[pushID]
	e.flightMu.Unlock()
	if f == nil {
		push, _ := e.getPush(ctx, pushID)
		return push
	}
	select {
	case <-f.done:
		return f.push
	case <-ctx.Done():
		return f.push
	}
}

func (e *Engine) completeFlight(push core.Push, err error) {
	e.flightMu.Lock()
	defer e.flightMu.Unlock()
	f := e.flights[push.PushID]
	if f == nil {
		return
	}
	f.push = push
	f.err = err
	close(f.done)
	delete(e.flights, push.PushID)
}

func (e *Engine) getPush(ctx context.Context, pushID string) (core.Push, error) {
	var push core.Push
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		var err error
		push, err = getPushTx(ctx, tx, pushID)
		return err
	})
	return push, err
}

func (e *Engine) finishBatch(ctx context.Context, batchID string) error {
	_, err := e.store.IntentWrite(ctx, "BATCH_FINISH", "batch:"+batchID, map[string]any{"batch_id": batchID}, func(tx *sql.Tx) (bool, error) {
		rows, err := tx.QueryContext(ctx, `SELECT status,error_code FROM pushes WHERE batch_id=? ORDER BY push_id`, batchID)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		statuses := map[string]int{}
		firstError := ""
		for rows.Next() {
			var status, code string
			if err := rows.Scan(&status, &code); err != nil {
				return false, err
			}
			statuses[status]++
			if firstError == "" && code != "" {
				firstError = code
			}
		}
		if err := rows.Err(); err != nil {
			return false, err
		}
		status := "DISPATCHED"
		if statuses[core.StatusAborted] > 0 {
			status = "COMPLETED_WITH_ERRORS"
		} else if statuses[core.StatusApplied] == len(statuses) && len(statuses) > 0 {
			status = "COMPLETED"
		}
		now := core.Now()
		_, err = tx.ExecContext(ctx, `UPDATE batches SET status=?,finished_at=?,error_code=? WHERE batch_id=?`, status, now, firstError, batchID)
		return err == nil, err
	})
	return err
}

func (e *Engine) triggerResult(ctx context.Context, created *createBatchResult) (*TriggerResult, error) {
	result := &TriggerResult{}
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		var finished sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT batch_id,version,status,created_at,finished_at,time_budget_ms,error_code FROM batches WHERE batch_id=?`, created.batch.BatchID).
			Scan(&result.Batch.BatchID, &result.Batch.Version, &result.Batch.Status, &result.Batch.CreatedAt, &finished, &result.Batch.TimeBudgetMS, &result.Batch.ErrorCode); err != nil {
			return err
		}
		if finished.Valid {
			result.Batch.FinishedAt = &finished.String
		}
		for _, pushID := range created.allPushIDs {
			push, err := getPushTx(ctx, tx, pushID)
			if err != nil {
				return err
			}
			view := PushView{Push: push, Reused: created.reused[pushID]}
			if ack, ackErr := getAckByPushTx(ctx, tx, pushID); ackErr == nil {
				view.Ack = &ack
			}
			result.Pushes = append(result.Pushes, view)
			if push.ErrorCode != "" {
				result.Errors = append(result.Errors, core.NewBizError(push.ErrorCode, push.ErrorMessage, push.ErrorContext))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) findPushByClientVersion(ctx context.Context, clientID string, version int64) (core.Push, error) {
	var push core.Push
	err := e.store.ReadTx(ctx, func(tx *sql.Tx) error {
		var err error
		push, err = scanPush(tx.QueryRowContext(ctx, `SELECT `+pushColumns+` FROM pushes WHERE client_id=? AND version=?`, clientID, version))
		if errors.Is(err, sql.ErrNoRows) {
			return core.NewBizError("E_PUSH_NOT_FOUND", "push not found", map[string]any{"client_id": clientID, "version": version})
		}
		return err
	})
	return push, err
}

func trimError(s string) string {
	if len(s) <= 512 {
		return s
	}
	return s[:512]
}
