package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"checkpointengine/internal/model"
)

func (e *Engine) SubmitIncrement(ctx context.Context, req model.IncrementRequest) (*model.SubmissionResponse, error) {
	req.Normalize()
	req.SubmissionID = strings.TrimSpace(req.SubmissionID)
	if req.SubmissionID == "" {
		return nil, model.NewAPIError(model.CodeBadRequest, "submission_id is required")
	}
	hash, err := requestHash(req)
	if err != nil {
		return nil, model.WrapAPIError(model.CodeBadRequest, "cannot canonicalize submission", err)
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	if existing, ok, err := e.loadSubmission(ctx, req.SubmissionID); err != nil {
		return nil, err
	} else if ok {
		if existing.RequestHash != hash {
			return nil, model.WithDetails(model.NewAPIError(model.CodeConflictingSubmit, "submission_id was already used with different content"),
				map[string]any{"submission_id": req.SubmissionID})
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
	if req.BaseVersion != state.Revision {
		return nil, model.WithDetails(model.NewAPIError(model.CodeStaleBase, "base_version is not the current committed revision"),
			map[string]any{"base_version": req.BaseVersion, "current_revision": state.Revision})
	}
	policyRec, err := e.loadPolicyDB(ctx, state.CurrentPolicyVersion)
	if err != nil {
		return nil, err
	}
	if policyRec == nil {
		return nil, model.NewAPIError(model.CodeNoPolicy, "submit a precision policy snapshot before a checkpoint increment")
	}
	policy := policyRec.Snapshot
	lossScale, lossScaleChanged, err := e.effectiveLossScale(req.LossScale, state.LossScale, policy.LossScaleInitial)
	if err != nil {
		return nil, err
	}
	growth, growthChanged, err := e.effectiveLossScale(req.LossScaleGrowth, state.LossScaleGrowth, policy.LossScaleGrowth)
	if err != nil {
		return nil, err
	}

	nowMS, now := e.nowMS(), e.nowText()
	prepared, err := prepareInputShards(append([]model.ShardInput(nil), req.Shards...), nowMS)
	if err != nil {
		return nil, err
	}
	all, err := loadAllShards(ctx, e.store.DB)
	if err != nil {
		return nil, err
	}
	existingMap := shardMap(all)
	changed := make(map[string]bool)
	shardsUpserted, shardsNoop := 0, 0
	vramPlan := stringSet(policy.VRAMResidency)
	persistedPlan := stringSet(policy.PersistedResidency)
	for i := range prepared {
		p := &prepared[i]
		key := shardKey(p.Round, p.ShardID)
		old, exists := existingMap[key]
		if exists {
			if old.Digest != p.Digest {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardMismatch, "shard content digest differs from the committed shard"),
					map[string]any{"round": p.Round, "shard_id": p.ShardID,
						"existing_digest": old.Digest, "submitted_digest": p.Digest})
			}
			if len(p.FP16Bits) == 0 && len(p.FP32Nanos) == 0 && p.PayloadHex == "" {
				p.FP16Bits = old.FP16Bits
				p.FP32Nanos = old.FP32Nanos
				p.PayloadHex = old.PayloadHex
			}
			p.Location = old.Location
			p.WriteState = old.WriteState
			p.Revision = old.Revision
			if p.RestoredFromDigest == "" {
				p.RestoredFromDigest = old.RestoredFromDigest
			}
			if p.DesiredPool == "" {
				p.DesiredPool = old.DesiredPool
			}
			if old.ContentMetadataEqual(*p) {
				shardsNoop++
			} else {
				shardsUpserted++
				changed[key] = true
			}
		} else {
			shardsUpserted++
			changed[key] = true
		}
		if vramPlan[p.ShardID] {
			p.DesiredPool = model.PoolVRAM
		} else if persistedPlan[p.ShardID] {
			p.DesiredPool = model.PoolPersisted
		}
		if p.DesiredPool == "" {
			p.DesiredPool = model.DefaultTarget(p.Precision)
		}
		p.Revision = state.Revision + 1
	}

	merged := append([]model.Shard(nil), all...)
	mergedByKey := shardMap(merged)
	for _, p := range prepared {
		mergedByKey[shardKey(p.Round, p.ShardID)] = p
	}
	merged = merged[:0]
	for _, s := range mergedByKey {
		merged = append(merged, s)
	}
	merged = sortedShards(merged)
	merged, err = resolveRelationships(merged)
	if err != nil {
		return nil, err
	}
	// Carry automatic resolution back into the incoming rows.
	resolvedMap := shardMap(merged)
	for i := range prepared {
		if r, ok := resolvedMap[shardKey(prepared[i].Round, prepared[i].ShardID)]; ok {
			prepared[i].RestoredFromDigest = r.RestoredFromDigest
		}
	}

	placement, err := placeShards(all, prepared, policy.VRAMLimitBytes)
	if err != nil {
		return nil, err
	}
	for _, ev := range placement.Evictions {
		changed[shardKey(ev.Round, ev.ShardID)] = true
	}

	precisionResults, report, err := e.computeAndCheckPrecision(merged, policy, lossScale, now)
	if err != nil {
		return nil, err
	}
	if !report.Passed {
		apiErr := model.NewAPIError(model.CodePrecisionDrift, "checkpoint rejected because precision drift exceeds the declared policy bounds")
		apiErr.Details = map[string]any{"precision": report}
		return nil, apiErr
	}

	sched, err := e.planSchedule(ctx, policy, placement.Shards)
	if err != nil {
		return nil, err
	}

	submissionKey := req.SubmissionID
	specs := []intentSpec{{Kind: "CHECKPOINT", SubmissionID: submissionKey,
		Before: map[string]any{"revision": state.Revision},
		After:  map[string]any{"round": req.Round, "shards": len(prepared), "revision": state.Revision + 1}}}
	for _, ev := range placement.Evictions {
		before := placement.EvictionFrom[shardKey(ev.Round, ev.ShardID)]
		specs = append(specs, intentSpec{Kind: "EVICT", SubmissionID: submissionKey,
			Before: before, After: evictionIntentJSON{Round: ev.Round, ShardID: ev.ShardID,
				Before: before, AfterPool: model.PoolPersisted, Reason: ev.Reason}})
	}
	blobPaths := make(map[string]string)
	for _, task := range sched.Writes {
		path := e.blobPathFor(task.Round, task.ShardID, task.Digest)
		blobPaths[model.StableTaskKey(task)] = path
		specs = append(specs, intentSpec{Kind: "WRITE", SubmissionID: submissionKey,
			Before: map[string]any{"round": task.Round, "shard_id": task.ShardID, "write_state": model.WritePending},
			After: writeIntentJSON{Round: task.Round, ShardID: task.ShardID, Digest: task.Digest,
				BlobPath: path, Precision: task.Precision, Bytes: task.Bytes}})
	}
	intentIDs, err := e.insertIntents(ctx, specs)
	if err != nil {
		return nil, err
	}
	if err := e.hitFailpoint("after_intents"); err != nil {
		return nil, err
	}

	for _, task := range sched.Writes {
		shard, ok := findShard(placement.Shards, task.Round, task.ShardID)
		if !ok {
			e.abortIntents(ctx, intentIDs)
			return nil, fmt.Errorf("scheduled missing shard %d/%s", task.Round, task.ShardID)
		}
		path, err := e.writeShardBlob(shard, req.SubmissionID)
		if err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, model.WrapAPIError(model.CodeWriteFailed, "durable shard write failed", err)
		}
		blobPaths[model.StableTaskKey(task)] = path
		if err := e.hitFailpoint("after_blob:" + task.ShardID); err != nil {
			return nil, err
		}
	}

	newRevision := state.Revision + 1
	tx, err := e.store.DB.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	defer tx.Rollback()

	finalMap := shardMap(placement.Shards)
	for _, p := range prepared {
		final, ok := finalMap[shardKey(p.Round, p.ShardID)]
		if !ok {
			e.abortIntents(ctx, intentIDs)
			return nil, fmt.Errorf("placement lost shard %d/%s", p.Round, p.ShardID)
		}
		if err := upsertShardTx(ctx, tx, final, now); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	for key, before := range placement.EvictionFrom {
		final, ok := finalMap[key]
		if !ok {
			continue
		}
		if before.Location == final.Location && before.DesiredPool == final.DesiredPool {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE shards SET location=?, desired_pool=?,
			revision=?, updated_at=? WHERE round=? AND shard_id=?`,
			final.Location, final.DesiredPool, newRevision, now, final.Round, final.ShardID); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	for key := range changed {
		if err := invalidateDerivedForShardsTx(ctx, tx, []string{key}); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	savedPrecision, err := savePrecisionResultsTx(ctx, tx, precisionResults)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	for _, task := range sched.Writes {
		path := blobPaths[model.StableTaskKey(task)]
		if _, err := tx.ExecContext(ctx, `INSERT INTO write_records(submission_id, round, shard_id,
			precision, digest, bytes, pool, committed_at, blob_path) VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(round, shard_id, digest) DO NOTHING`, req.SubmissionID, task.Round,
			task.ShardID, task.Precision, task.Digest, task.Bytes, poolForShard(placement.Shards, task.Round, task.ShardID), now, path); err != nil {
			e.abortIntents(ctx, intentIDs)
			return nil, err
		}
	}
	if err := applyScheduleTx(ctx, tx, req.SubmissionID, sched, now, nowMS); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE engine_state SET revision=?, loss_scale=?, loss_scale_growth=? WHERE id=1`,
		newRevision, lossScale, growth); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := rebuildPoolUsageTx(ctx, tx, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	policyForDerivation := policy
	policyForDerivation.LossScaleInitial = lossScale
	policyForDerivation.LossScaleGrowth = growth
	if _, err := rebuildAllDerivedTx(ctx, tx, policyForDerivation, newRevision, now); err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}

	response := model.SubmissionResponse{
		SubmissionID: req.SubmissionID, Revision: newRevision, BaseVersion: req.BaseVersion,
		Round: req.Round, PolicyVersion: policy.Version, LossScale: lossScale,
		ShardsUpserted: shardsUpserted, ShardsNoop: shardsNoop,
		Evictions: placement.Evictions, Writes: sched.Writes, Deferred: sched.Deferred,
		QueueDepth: sched.QueueDepth, WriteOrderBasis: sched.OrderBasis,
		RemainingBytes: sched.RemainingBytes, RemainingTimeMS: sched.RemainingTimeMS,
		DerivedRebuilt: changedKeyLabels(changed), Precision: savedPrecision,
	}
	resultJSON, err := json.Marshal(response)
	if err != nil {
		e.abortIntents(ctx, intentIDs)
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO submissions(submission_id, kind, request_hash,
		status, result_json, revision, created_at, completed_at) VALUES(?, 'CHECKPOINT', ?, 'COMPLETE', ?, ?, ?, ?)`,
		req.SubmissionID, hash, string(resultJSON), newRevision, now, now); err != nil {
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
	if lossScaleChanged || growthChanged {
		response.IdempotentReplay = false
	}
	return &response, nil
}

type submissionRow struct {
	RequestHash string
	Status      string
	ResultJSON  string
}

func (e *Engine) loadSubmission(ctx context.Context, id string) (submissionRow, bool, error) {
	var s submissionRow
	err := e.store.DB.QueryRowContext(ctx, `SELECT request_hash, status, result_json FROM submissions WHERE submission_id=?`, id).
		Scan(&s.RequestHash, &s.Status, &s.ResultJSON)
	if isNoRows(err) {
		return s, false, nil
	}
	if err != nil {
		return s, false, err
	}
	return s, true, nil
}

func (e *Engine) loadPolicyDB(ctx context.Context, version int64) (*policyRecord, error) {
	if version == 0 {
		return nil, nil
	}
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return loadPolicyTx(ctx, tx, version)
}

func (e *Engine) effectiveLossScale(raw json.RawMessage, current, fallback string) (string, bool, error) {
	if len(strings.TrimSpace(string(raw))) == 0 || string(raw) == "null" {
		if current != "" {
			return current, false, nil
		}
		return fallback, false, nil
	}
	value, err := model.DecimalValue(raw)
	if err != nil {
		return "", false, model.WrapAPIError(model.CodeBadRequest, "invalid loss scale field", err)
	}
	return value, value != current, nil
}

func (e *Engine) planSchedule(ctx context.Context, policy model.Snapshot, shards []model.Shard) (scheduleResult, error) {
	tx, err := e.store.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return scheduleResult{}, err
	}
	defer tx.Rollback()
	return scheduleWrites(ctx, tx, policy, shards)
}

func upsertShardTx(ctx context.Context, tx *sql.Tx, s model.Shard, now string) error {
	bits, err := json.Marshal(s.FP16Bits)
	if err != nil {
		return err
	}
	nanos, err := json.Marshal(s.FP32Nanos)
	if err != nil {
		return err
	}
	if len(s.FP16Bits) == 0 {
		bits = nil
	}
	if len(s.FP32Nanos) == 0 {
		nanos = nil
	}
	bitsJSON, nanosJSON := "", ""
	if len(bits) > 0 {
		bitsJSON = string(bits)
	}
	if len(nanos) > 0 {
		nanosJSON = string(nanos)
	}
	pinned := 0
	if s.Pinned {
		pinned = 1
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO shards(round, shard_id, parameter_id, precision,
		bytes, digest, momentum_digest, restored_from_digest, location, desired_pool,
		write_state, last_access_ms, priority, write_ms, pinned, quantization,
		fp16_bits_json, fp32_nanos_json, payload_hex, revision, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(round, shard_id) DO UPDATE SET parameter_id=excluded.parameter_id,
		precision=excluded.precision, bytes=excluded.bytes, digest=excluded.digest,
		momentum_digest=excluded.momentum_digest, restored_from_digest=excluded.restored_from_digest,
		location=excluded.location, desired_pool=excluded.desired_pool, write_state=excluded.write_state,
		last_access_ms=excluded.last_access_ms, priority=excluded.priority, write_ms=excluded.write_ms,
		pinned=excluded.pinned, quantization=excluded.quantization, fp16_bits_json=excluded.fp16_bits_json,
		fp32_nanos_json=excluded.fp32_nanos_json, payload_hex=excluded.payload_hex,
		revision=excluded.revision, updated_at=excluded.updated_at`,
		s.Round, s.ShardID, s.ParameterID, s.Precision, s.Bytes, s.Digest, s.MomentumDigest,
		s.RestoredFromDigest, s.Location, s.DesiredPool, s.WriteState, s.LastAccessMS,
		s.Priority, s.WriteMS, pinned, s.Quantization, bitsJSON, nanosJSON, s.PayloadHex,
		s.Revision, now)
	return err
}

func poolForShard(shards []model.Shard, round int, id string) string {
	if s, ok := findShard(shards, round, id); ok {
		return s.Location
	}
	return model.PoolPersisted
}

func changedKeyLabels(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, strings.ReplaceAll(key, "\x00", "/"))
	}
	sort.Strings(out)
	return out
}
