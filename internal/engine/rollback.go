package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
)

func (e *Engine) dispatchRollback(ctx context.Context, batch domain.Batch, policy domain.Policy,
	evaluation risk.Evaluation, event string) *RollbackDispatch {
	dispatch := &RollbackDispatch{TargetURL: e.rollbackURL, Attempted: true, RequestedAtMS: e.now()}
	if strings.TrimSpace(e.rollbackURL) == "" {
		dispatch.Error = "no rollback target configured"
		return dispatch
	}
	payload := map[string]any{
		"event": event, "batch_id": batch.ID, "service": batch.Service,
		"policy_version": policy.Version, "score_bp": evaluation.ScoreBP, "level": evaluation.Level,
		"reason": evaluation.Decision.Reason, "requested_at_ms": dispatch.RequestedAtMS,
	}
	raw, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.rollbackURL, bytes.NewReader(raw))
	if err != nil {
		dispatch.Error = err.Error()
		return dispatch
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Rollout-Guard-Event", event)
	start := time.Now()
	response, err := e.client.Do(request)
	dispatch.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		dispatch.Error = err.Error()
		return dispatch
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	dispatch.StatusCode = response.StatusCode
	dispatch.Success = response.StatusCode >= 200 && response.StatusCode < 300
	if !dispatch.Success {
		dispatch.Error = fmt.Sprintf("webhook returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return dispatch
}

func (e *Engine) applyFinalStatus(ctx context.Context, batchID, status string, intentID int64,
	intentStatus, note string, dispatch *RollbackDispatch) error {
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := e.now()
	commit, err := store.NextCommit(ctx, tx, "rollback_finalize", now)
	if err != nil {
		return err
	}
	if err := store.UpdateBatchStatusTx(ctx, tx, batchID, status, now); err != nil {
		return err
	}
	detail := map[string]any{"final_status": status, "commit_seq": commit}
	if dispatch != nil {
		detail["webhook_success"] = dispatch.Success
		detail["webhook_status_code"] = dispatch.StatusCode
	}
	if err := store.SetIntentTx(ctx, tx, intentID, intentStatus, note, detail); err != nil {
		return err
	}
	return tx.Commit()
}
