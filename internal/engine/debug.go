package engine

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"mpckpt/internal/model"
)

// PrepareCrashFault deliberately leaves a durable RUNNING intent and partial
// shard state behind. The verification tool kills the process immediately after
// this method returns; restart must execute recoverIntents.
func (e *Engine) PrepareCrashFault(commitID string) (json.RawMessage, *model.BizError) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if strings.TrimSpace(commitID) == "" {
		return nil, model.NewError(model.CodeSpecInvalid, "commit_id is required", map[string]any{"field": "commit_id"})
	}
	current, berr := e.loadCurrentShards(context.Background(), e.store.DB())
	if berr != nil {
		return nil, berr
	}
	baseline, berr := e.loadCurrentBaseline(context.Background(), e.store.DB())
	if berr != nil {
		return nil, berr
	}
	protected := baselineSet(baseline)
	ids := make([]string, 0, len(current))
	for id := range current {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var pendingID string
	for _, id := range ids {
		shard := current[id]
		if shard.Status != model.StatusResident && shard.Precision == model.PrecisionFP32 {
			pendingID = id
			break
		}
	}
	var evictionShard model.Shard
	for _, id := range ids {
		shard := current[id]
		if shard.Status == model.StatusResident && !protected[id] {
			evictionShard = shard
			break
		}
	}
	var undo undoState
	if pendingID != "" {
		undo.PendingShardIDs = append(undo.PendingShardIDs, pendingID)
	}
	if evictionShard.ID != "" {
		undo.Evictions = append(undo.Evictions, struct {
			ShardID string     `json:"shard_id"`
			Pool    model.Pool `json:"pool"`
			Status  string     `json:"status"`
		}{ShardID: evictionShard.ID, Pool: evictionShard.Pool, Status: string(evictionShard.Status)})
	}
	undoJSON, _ := json.Marshal(undo)
	hash, berr := e.hashRequest(struct {
		Kind string `json:"kind"`
	}{"CRASH_FAULT"})
	if berr != nil {
		return nil, berr
	}
	start, berr := e.beginCommit(commitID, "CRASH_FAULT", hash, string(undoJSON))
	if berr != nil {
		return nil, berr
	}
	if start.ReplayError != nil {
		return nil, start.ReplayError
	}
	if start.ReplayResponse != nil {
		return start.ReplayResponse, nil
	}
	tx, err := e.store.DB().BeginTx(context.Background(), nil)
	if err != nil {
		return nil, e.failure(commitID, internalError("begin crash fault transaction", err))
	}
	defer tx.Rollback()
	if pendingID != "" {
		if _, err := tx.Exec(`UPDATE shards SET status = 'WRITING', write_intent = ?, updated_at = ? WHERE id = ?`, commitID, e.store.Now(), pendingID); err != nil {
			return nil, e.failureAfterTx(tx, commitID, internalError("write fault pending shard", err))
		}
	}
	if evictionShard.ID != "" {
		if _, err := tx.Exec(`UPDATE shards SET pool = 'PERSISTED', status = 'PERSISTED', write_intent = ?, updated_at = ? WHERE id = ?`, commitID, e.store.Now(), evictionShard.ID); err != nil {
			return nil, e.failureAfterTx(tx, commitID, internalError("write fault eviction", err))
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, e.failure(commitID, internalError("commit crash fault", err))
	}
	response, _ := json.Marshal(map[string]any{
		"commit_id":     commitID,
		"ready":         true,
		"pending_shard": pendingID,
		"evicted_shard": evictionShard.ID,
		"intent_status": "RUNNING",
	})
	return response, nil
}
