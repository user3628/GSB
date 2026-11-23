package engine

import (
	"context"
	"sort"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/store"
)

type IngestResult struct {
	BatchID             string  `json:"batch_id"`
	Accepted            int     `json:"accepted"`
	Idempotent          int     `json:"idempotent"`
	CommitSeq           int64   `json:"commit_seq"`
	ObservationRevision int64   `json:"observation_revision"`
	PolicyVersion       int64   `json:"policy_version"`
	LastObservationMS   int64   `json:"last_observation_ts_ms"`
	IdempotentSeqs      []int64 `json:"idempotent_seqs,omitempty"`
	IntentID            int64   `json:"intent_id"`
}

func (e *Engine) IngestObservations(ctx context.Context, batchID string, observations []domain.Observation) (IngestResult, error) {
	if len(observations) == 0 {
		return IngestResult{}, domain.NewBizError("E_OBSERVATION_INVALID", "观测批次不能为空", map[string]any{"batch_id": batchID})
	}
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			return IngestResult{}, err
		}
	}
	intentID, err := store.CreateIntent(ctx, e.store, "observation_ingest", batchID, map[string]any{"reports": len(observations)})
	if err != nil {
		return IngestResult{}, err
	}
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback()
	policy, _, err := store.CurrentPolicy(ctx, tx)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "policy missing")
		return IngestResult{}, err
	}
	batch, err := store.GetBatch(ctx, tx, batchID)
	if err != nil {
		_ = failIntentTx(ctx, tx, intentID, "batch not found")
		return IngestResult{}, err
	}
	if batch.Status != domain.StatusObserving {
		_ = failIntentTx(ctx, tx, intentID, "batch is terminal")
		return IngestResult{}, domain.NewBizError("E_INVALID_TRANSITION", "仅 OBSERVING 批次可以摄入新观测", map[string]any{
			"batch_id": batchID, "from": batch.Status, "to": "OBSERVING",
		})
	}

	stored := map[int64]string{}
	for _, observation := range observations {
		if _, seen := stored[observation.Seq]; seen {
			continue
		}
		hash, err := store.ObservationBySeq(ctx, tx, batchID, observation.Seq)
		if err != nil {
			return IngestResult{}, err
		}
		if hash != "" {
			stored[observation.Seq] = hash
		}
	}
	seen := make(map[int64]string, len(stored))
	for seq, hash := range stored {
		seen[seq] = hash
	}
	newObservations := make([]domain.Observation, 0)
	idempotentSeqs := make([]int64, 0)
	requestMax := batch.LastObservationTSMS
	for _, observation := range observations {
		hash := observation.PayloadHash()
		if previous, ok := seen[observation.Seq]; ok {
			if previous != hash {
				_ = failIntentTx(ctx, tx, intentID, "conflicting duplicate report")
				return IngestResult{}, domain.NewBizError("E_CONFLICTING_REPORT", "同一批次同一序号内容不一致，首次上报已保留", map[string]any{
					"batch_id": batchID, "seq": observation.Seq, "stored_hash": previous, "received_hash": hash,
				})
			}
			idempotentSeqs = append(idempotentSeqs, observation.Seq)
			continue
		}
		if observation.TimestampMS < requestMax {
			_ = failIntentTx(ctx, tx, intentID, "timestamp regression rejected")
			return IngestResult{}, domain.NewBizError("E_OBSERVATION_INVALID", "观测时间戳回退，整批拒绝", map[string]any{
				"batch_id": batchID, "seq": observation.Seq, "timestamp_ms": observation.TimestampMS,
				"last_timestamp_ms": requestMax,
			})
		}
		requestMax = observation.TimestampMS
		seen[observation.Seq] = hash
		newObservations = append(newObservations, observation)
	}
	sort.Slice(idempotentSeqs, func(i, j int) bool { return idempotentSeqs[i] < idempotentSeqs[j] })
	now := e.now()
	if len(newObservations) == 0 {
		if err := store.SetIntentTx(ctx, tx, intentID, "COMPLETED", "all reports idempotent", nil); err != nil {
			return IngestResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return IngestResult{}, err
		}
		commit, err := store.CurrentCommit(ctx, e.store.DB())
		if err != nil {
			return IngestResult{}, err
		}
		return IngestResult{
			BatchID: batchID, Accepted: 0, Idempotent: len(idempotentSeqs), CommitSeq: commit,
			ObservationRevision: batch.Revision, PolicyVersion: policy.Version,
			LastObservationMS: batch.LastObservationTSMS, IdempotentSeqs: idempotentSeqs, IntentID: intentID,
		}, nil
	}
	commit, err := store.NextCommit(ctx, tx, "observation_ingest", now)
	if err != nil {
		return IngestResult{}, err
	}
	for _, observation := range newObservations {
		bucketStart := observation.TimestampMS - observation.TimestampMS%policy.BucketMS
		if err := store.InsertObservationTx(ctx, tx, batchID, observation, bucketStart, policy.Version, commit, now); err != nil {
			return IngestResult{}, err
		}
	}
	if err := store.TouchBatchAfterIngestTx(ctx, tx, batchID, requestMax, now); err != nil {
		return IngestResult{}, err
	}
	if err := store.SetIntentTx(ctx, tx, intentID, "COMPLETED", "observations committed as one batch", map[string]any{
		"accepted": len(newObservations), "commit_seq": commit,
	}); err != nil {
		return IngestResult{}, err
	}
	e.maybeCrash("observation_before_commit", intentID, batchID)
	if err := tx.Commit(); err != nil {
		return IngestResult{}, err
	}
	return IngestResult{
		BatchID: batchID, Accepted: len(newObservations), Idempotent: len(idempotentSeqs), CommitSeq: commit,
		ObservationRevision: batch.Revision + 1, PolicyVersion: policy.Version, LastObservationMS: requestMax,
		IdempotentSeqs: idempotentSeqs, IntentID: intentID,
	}, nil
}
