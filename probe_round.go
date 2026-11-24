package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
)

type ProbeRoundRequest struct {
	CommitID        string            `json:"commit_id,omitempty"`
	BatchID         string            `json:"batch_id,omitempty"`
	RouteKeys       []string          `json:"route_keys"`
	BaseURL         string            `json:"base_url,omitempty"`
	Endpoints       map[string]string `json:"endpoints,omitempty"`
	TimeoutMS       int64             `json:"timeout_ms,omitempty"`
	LateThresholdMS int64             `json:"late_threshold_ms,omitempty"`
	AsOfMS          int64             `json:"as_of_ms,omitempty"`
	ExpectedOwners  map[string]string `json:"expected_owners,omitempty"`
	Tolerance       string            `json:"tolerance,omitempty"`
	Injection       *ProbeInjection   `json:"injection,omitempty"`
}

type ProbeRoundResult struct {
	CommitID         string        `json:"commit_id"`
	BatchID          string        `json:"batch_id"`
	ViewVersion      int64         `json:"view_version"`
	SnapshotVersion  int64         `json:"snapshot_version"`
	TotalRoutes      int           `json:"total_routes"`
	OKCount          int           `json:"ok_count"`
	FailureCount     int           `json:"failure_count"`
	Results          []ProbeResult `json:"results"`
	IdempotentReplay bool          `json:"idempotent_replay,omitempty"`
}

func (r *ProbeRoundResult) MarkIdempotentReplay() {
	if r != nil {
		r.IdempotentReplay = true
	}
}

func (w *Workbench) SubmitProbeRound(ctx context.Context, request ProbeRoundRequest) (*ProbeRoundResult, *Error) {
	commitID := request.CommitID
	if commitID == "" {
		commitID = request.BatchID
	}
	if commitID == "" {
		commitID = randomSuffix("probe-commit-")
	}
	batchID := request.BatchID
	if batchID == "" {
		batchID = commitID
	}
	request.CommitID = commitID
	request.BatchID = batchID
	if len(request.RouteKeys) == 0 {
		routes, _, err := w.ListRoutes(ctx)
		if err != nil {
			return nil, err
		}
		request.RouteKeys = routes
	}
	requestHash := hashRequest(request)

	w.idemMu.Lock()
	if call, ok := w.inflight[commitID]; ok {
		if call.hash != requestHash {
			w.idemMu.Unlock()
			return nil, NewError(CodeConflictingLedger, "probe commit_id was already used with different content").
				WithContext("commit_id", commitID)
		}
		w.idemMu.Unlock()
		<-call.done
		if call.bizErr != nil {
			return nil, call.bizErr
		}
		var result ProbeRoundResult
		if err := json.Unmarshal(call.resultJSON, &result); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		result.IdempotentReplay = true
		return &result, nil
	}
	call := &inflightCall{done: make(chan struct{}), hash: requestHash}
	w.inflight[commitID] = call
	w.idemMu.Unlock()

	result, bizErr := w.runProbeRoundLeader(ctx, request, commitID, batchID, requestHash)
	if bizErr != nil {
		call.bizErr = bizErr
	} else {
		call.resultJSON, _ = json.Marshal(result)
	}
	close(call.done)
	w.idemMu.Lock()
	delete(w.inflight, commitID)
	w.idemMu.Unlock()
	return result, bizErr
}

func (w *Workbench) runProbeRoundLeader(
	ctx context.Context, request ProbeRoundRequest, commitID, batchID, requestHash string,
) (*ProbeRoundResult, *Error) {
	select {
	case w.writer <- struct{}{}:
	case <-ctx.Done():
		return nil, WrapError(CodeInternal, ctx.Err())
	}
	defer func() { <-w.writer }()

	nowMS := w.NowMS()
	existing, idemErr := loadIdempotency(ctx, w.db, commitID)
	if idemErr != nil {
		return nil, idemErr
	}
	attempt := 1
	if existing != nil {
		if existing.RequestHash != requestHash {
			return nil, NewError(CodeConflictingLedger, "commit_id was already used with different content").
				WithContextMap(map[string]any{"commit_id": commitID, "batch_id": batchID})
		}
		if existing.Status == "committed" && existing.ResultJSON.Valid {
			var result ProbeRoundResult
			if jsonErr := json.Unmarshal([]byte(existing.ResultJSON.String), &result); jsonErr != nil {
				return nil, WrapError(CodeInternal, jsonErr)
			}
			result.IdempotentReplay = true
			return &result, nil
		}
		if existing.Status == "rejected" && existing.ErrorJSON.Valid {
			var replay Error
			if jsonErr := json.Unmarshal([]byte(existing.ErrorJSON.String), &replay); jsonErr == nil {
				replay.httpStatus = DefaultHTTPStatus(replay.Code)
				return nil, &replay
			}
		}
		if existing.Status == "pending" || existing.Status == "recovered" {
			attempt = existing.Attempt + 1
		}
	}

	viewVersion, err := w.store.ViewVersion(ctx)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	snapshotVersion, err := w.store.SnapshotVersion(ctx)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	asOf := request.AsOfMS
	if asOf == 0 {
		asOf = nowMS
	}
	tolerance, tolErr := ParseTolerance(request.Tolerance)
	if tolErr != nil {
		return nil, WrapError(CodeLedgerInvalid, tolErr)
	}

	intentID := fmt.Sprintf("intent:%s:%d", commitID, attempt)
	payloadJSON, _ := json.Marshal(map[string]any{
		"batch_id": batchID, "request_hash": requestHash, "request": request,
	})
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO idempotency(commit_id,request_hash,kind,status,attempt,created_at_ms,updated_at_ms)
		VALUES(?,?, 'probe','pending',?,?,?)
		ON CONFLICT(commit_id) DO UPDATE SET status='pending',attempt=excluded.attempt,updated_at_ms=excluded.updated_at_ms`,
		commitID, requestHash, attempt, nowMS, nowMS)
	if err == nil {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
			VALUES(?,?, 'probe',?,?,'probing',?,?)
			ON CONFLICT(intent_id) DO UPDATE SET status='probing',payload_json=excluded.payload_json,updated_at_ms=excluded.updated_at_ms`,
			intentID, commitID, attempt, string(payloadJSON), nowMS, nowMS)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO probe_batches(batch_id,commit_id,view_version,snapshot_version,endpoint_base,config_json,status,total_routes,created_at_ms,updated_at_ms)
			VALUES(?,?,?,?,?,?,'started',?,?,?)
			ON CONFLICT(batch_id) DO UPDATE SET status='started',config_json=excluded.config_json,updated_at_ms=excluded.updated_at_ms`,
			batchID, commitID, viewVersion, snapshotVersion, request.BaseURL, string(payloadJSON),
			len(request.RouteKeys), nowMS, nowMS)
	}
	if err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}

	expectedOwners := request.ExpectedOwners
	if expectedOwners == nil {
		expectedOwners = map[string]string{}
	}
	for _, routeKey := range request.RouteKeys {
		if expectedOwners[routeKey] != "" {
			continue
		}
		view, viewErr := w.GetConservation(ctx, routeKey, viewVersion, asOf, request.Tolerance)
		if viewErr == nil {
			expectedOwners[routeKey] = dominantOwner(view)
		}
	}

	results := make([]ProbeResult, len(request.RouteKeys))
	var waitGroup sync.WaitGroup
	for index, routeKey := range request.RouteKeys {
		waitGroup.Add(1)
		go func(index int, routeKey string) {
			defer waitGroup.Done()
			probeRequest := ProbeRequest{
				BatchID: batchID, RouteKey: routeKey, BaseURL: request.BaseURL,
				Endpoint: request.Endpoints[routeKey], ExpectedOwner: expectedOwners[routeKey],
				TimeoutMS: request.TimeoutMS, LateThresholdMS: request.LateThresholdMS,
				Injection: request.Injection,
			}
			result := w.PerformProbe(ctx, probeRequest)
			results[index] = result
			w.persistProbeResultFact(ctx, result, nowMS)
		}(index, routeKey)
	}
	waitGroup.Wait()

	round := &ProbeRoundResult{
		CommitID: commitID, BatchID: batchID, ViewVersion: viewVersion,
		SnapshotVersion: snapshotVersion, TotalRoutes: len(results), Results: results,
	}
	for _, result := range results {
		if result.OK {
			round.OKCount++
		} else {
			round.FailureCount++
		}
	}

	commitTx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	if err := invalidateDerivedTx(ctx, commitTx, request.RouteKeys, "probe_round_completed", nowMS); err != nil {
		_ = commitTx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	writeContext := &writeContext{Ctx: ctx, Tx: commitTx, CommitID: commitID, IntentID: intentID,
		Attempt: attempt, CurrentView: viewVersion, NextView: viewVersion, NowMS: nowMS}
	if _, bizErr := rebuildViewsTx(writeContext, snapshotVersion, asOf, tolerance, request.RouteKeys); bizErr != nil {
		_ = commitTx.Rollback()
		return nil, bizErr
	}
	for _, result := range results {
		if err := upsertProbeResultTx(ctx, commitTx, result, nowMS, true); err != nil {
			_ = commitTx.Rollback()
			return nil, WrapError(CodeInternal, err)
		}
		if _, err := commitTx.ExecContext(ctx, `
			UPDATE derived_lineage SET probe_batch_id=?, probe_status=?
			WHERE view_version=? AND route_key=?`,
			batchID, result.Code, viewVersion, result.RouteKey); err != nil {
			_ = commitTx.Rollback()
			return nil, WrapError(CodeInternal, err)
		}
	}
	roundJSON, _ := json.Marshal(round)
	if _, err := commitTx.ExecContext(ctx, `
		UPDATE probe_batches SET status='completed',completed_routes=?,updated_at_ms=? WHERE batch_id=?`,
		len(results), nowMS, batchID); err != nil {
		_ = commitTx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if _, err := commitTx.ExecContext(ctx,
		`UPDATE intents SET status='committed',view_version=?,updated_at_ms=? WHERE intent_id=?`,
		viewVersion, nowMS, intentID); err != nil {
		_ = commitTx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if _, err := commitTx.ExecContext(ctx, `
		UPDATE idempotency SET status='committed',result_json=?,error_json=NULL,view_version=?,updated_at_ms=? WHERE commit_id=?`,
		string(roundJSON), viewVersion, nowMS, commitID); err != nil {
		_ = commitTx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if err := commitTx.Commit(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	return round, nil
}

func dominantOwner(view *ConservationView) string {
	owner := ""
	bestWeight := -1
	bestHit := int64(-1)
	for _, version := range view.Versions {
		if version.SnapshotWeightPermille > bestWeight ||
			(version.SnapshotWeightPermille == bestWeight && version.HitWeight > bestHit) {
			bestWeight = version.SnapshotWeightPermille
			bestHit = version.HitWeight
			owner = version.Version
		}
	}
	if bestWeight <= 0 {
		for _, version := range view.Versions {
			if version.HitWeight > bestHit {
				bestHit = version.HitWeight
				owner = version.Version
			}
		}
	}
	return owner
}

func (w *Workbench) persistProbeResultFact(ctx context.Context, result ProbeResult, nowMS int64) {
	_ = upsertProbeResult(ctx, w.db, result, nowMS, false)
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func upsertProbeResult(ctx context.Context, exec sqlExecer, result ProbeResult, nowMS int64, current bool) error {
	return upsertProbeResultRow(ctx, exec, result, nowMS, current)
}

func upsertProbeResultTx(ctx context.Context, exec sqlExecer, result ProbeResult, nowMS int64, current bool) error {
	return upsertProbeResultRow(ctx, exec, result, nowMS, current)
}

func upsertProbeResultRow(ctx context.Context, exec sqlExecer, result ProbeResult, nowMS int64, current bool) error {
	detailJSON, _ := json.Marshal(result.Detail)
	isCurrent := 0
	if current {
		isCurrent = 1
	}
	_, err := exec.ExecContext(ctx, `
		INSERT INTO probe_results(
			batch_id,route_key,endpoint,observed_owner,outcome_code,http_status,elapsed_ms,
			timed_out,conn_closed,slot_released,detail_json,is_current,persisted_at_ms)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(batch_id,route_key) DO UPDATE SET
			endpoint=excluded.endpoint,
			observed_owner=excluded.observed_owner,
			outcome_code=excluded.outcome_code,
			http_status=excluded.http_status,
			elapsed_ms=excluded.elapsed_ms,
			timed_out=excluded.timed_out,
			conn_closed=excluded.conn_closed,
			slot_released=excluded.slot_released,
			detail_json=excluded.detail_json,
			is_current=excluded.is_current,
			persisted_at_ms=excluded.persisted_at_ms`,
		result.BatchID, result.RouteKey, result.Endpoint, result.ObservedOwner, result.Code,
		result.HTTPStatus, result.ElapsedMS, boolToInt(result.TimedOut), boolToInt(result.ConnectionClosed),
		boolToInt(result.SlotReleased), string(detailJSON), isCurrent, nowMS)
	return err
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
