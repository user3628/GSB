package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"checkpointengine/internal/model"
)

// SchedulePending drains the bounded write queue using the currently committed
// policy. It never invents progress: shards that do not fit remain PENDING.
func (e *Engine) SchedulePending(ctx context.Context, submissionID string) (*model.SubmissionResponse, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	submissionID = strings.TrimSpace(submissionID)
	if submissionID == "" {
		submissionID = fmt.Sprintf("schedule:%d", e.nowMS())
	}
	const scheduleHash = "schedule-pending-v1"
	if existing, ok, err := e.loadSubmission(ctx, submissionID); err != nil {
		return nil, err
	} else if ok {
		if existing.RequestHash != scheduleHash {
			return nil, model.WithDetails(model.NewAPIError(model.CodeConflictingSubmit, "submission_id was already used for a different operation"),
				map[string]any{"submission_id": submissionID})
		}
		var response model.SubmissionResponse
		if err := json.Unmarshal([]byte(existing.ResultJSON), &response); err != nil {
			return nil, err
		}
		response.IdempotentReplay = true
		return &response, nil
	}
	state, err := loadStateDB(ctx, e.store.DB)
	if err != nil {
		return nil, err
	}
	if state.CurrentPolicyVersion == 0 {
		return nil, model.NewAPIError(model.CodeNoPolicy, "no policy is committed")
	}
	policyRec, err := e.loadPolicyDB(ctx, state.CurrentPolicyVersion)
	if err != nil {
		return nil, err
	}
	shards, err := loadAllShards(ctx, e.store.DB)
	if err != nil {
		return nil, err
	}
	sched, err := e.planSchedule(ctx, policyRec.Snapshot, shards)
	if err != nil {
		return nil, err
	}
	response := &model.SubmissionResponse{
		SubmissionID: submissionID, Revision: state.Revision, BaseVersion: state.Revision,
		PolicyVersion: policyRec.Version, LossScale: state.LossScale,
		Writes: sched.Writes, Deferred: sched.Deferred, QueueDepth: sched.QueueDepth,
		WriteOrderBasis: sched.OrderBasis, RemainingBytes: sched.RemainingBytes,
		RemainingTimeMS: sched.RemainingTimeMS,
	}
	if len(sched.Writes) == 0 {
		return response, nil
	}
	now, nowMS := e.nowText(), e.nowMS()
	var specs []intentSpec
	paths := make(map[string]string)
	for _, task := range sched.Writes {
		path := e.blobPathFor(task.Round, task.ShardID, task.Digest)
		paths[model.StableTaskKey(task)] = path
		specs = append(specs, intentSpec{Kind: "WRITE", SubmissionID: submissionID,
			Before: map[string]any{"round": task.Round, "shard_id": task.ShardID, "write_state": model.WritePending},
			After: writeIntentJSON{Round: task.Round, ShardID: task.ShardID, Digest: task.Digest,
				BlobPath: path, Precision: task.Precision, Bytes: task.Bytes}})
	}
	intentIDs, err := e.insertIntents(ctx, specs)
	if err != nil {
		return nil, err
	}
	if err := e.hitFailpoint("after_schedule_intents"); err != nil {
		return nil, err
	}
	for _, task := range sched.Writes {
		shard, ok := findShard(shards, task.Round, task.ShardID)
		if !ok {
			e.abortIntents(ctx, intentIDs)
			return nil, fmt.Errorf("queued shard missing: %d/%s", task.Round, task.ShardID)
		}
		path, err := e.writeShardBlob(shard, submissionID)
		if err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, model.WrapAPIError(model.CodeWriteFailed, "durable shard write failed", err)
		}
		paths[model.StableTaskKey(task)] = path
	}
	newRevision := state.Revision + 1
	response.Revision = newRevision
	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	defer tx.Rollback()
	for _, task := range sched.Writes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO write_records(submission_id, round, shard_id,
			precision, digest, bytes, pool, committed_at, blob_path) VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(round, shard_id, digest) DO NOTHING`, submissionID, task.Round,
			task.ShardID, task.Precision, task.Digest, task.Bytes,
			poolForShard(shards, task.Round, task.ShardID), now, paths[model.StableTaskKey(task)]); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	if err := applyScheduleTx(ctx, tx, submissionID, sched, now, nowMS); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engine_state SET revision=? WHERE id=1`, newRevision); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := rebuildPoolUsageTx(ctx, tx, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := rebuildAllDerivedTx(ctx, tx, policyRec.Snapshot, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	resultJSON, err := json.Marshal(response)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO submissions(submission_id, kind, request_hash,
		status, result_json, revision, created_at, completed_at) VALUES(?, 'SCHEDULE', ?, 'COMPLETE', ?, ?, ?, ?)`,
		submissionID, scheduleHash, string(resultJSON), newRevision, now, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if err := markIntentsApplied(ctx, tx, intentIDs, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	return response, nil
}
